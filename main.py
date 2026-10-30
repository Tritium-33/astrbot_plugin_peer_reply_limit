"""QQ sender quotas, independent of wake words, models and other cooldowns."""

from functools import wraps

from astrbot.api import AstrBotConfig, logger
from astrbot.api.message_components import Plain
from astrbot.api.platform import MessageType
from astrbot.api.star import Context, Star

from .quota import Quotas

NAME = "astrbot_plugin_peer_reply_limit"
TURN = "peer_reply_limit.turn"
DENIED = "peer_reply_limit.denied"


class PeerReplyLimit(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.seconds = int(config.get("window_seconds", 1800))
        self.maximum = int(config.get("max_replies", 3))
        if self.seconds < 1 or self.maximum < 1:
            raise ValueError("统计秒数和回复上限必须为正整数。")
        self.targets = {str(x).strip() for x in config.get("target_ids", [])}
        self.targets.discard("")
        self.groups = {str(x).strip() for x in config.get("group_ids", [])}
        self.groups.discard("")
        self.quotas = Quotas()
        self.active = False
        self.patches = []

    def applies(self, event):
        if (
            not self.config.get("enabled", True)
            or event.get_message_type() != MessageType.GROUP_MESSAGE
            or event.get_platform_name() != "aiocqhttp"
            or str(event.get_sender_id()) not in self.targets
            or (self.groups and str(event.get_group_id()) not in self.groups)
        ):
            return False
        selected = getattr(event, "plugins_name", None)
        if selected is not None and NAME not in selected:
            return False
        # Use AstrBot's configured administrator IDs, not QQ group roles/names.
        cfg = self.context.get_config(umo=event.unified_msg_origin)
        return str(event.get_sender_id()) not in {
            str(x) for x in cfg.get("admins_id", [])
        }

    def admit(self, event):
        if not self.applies(event):
            return True
        if event.get_extra(DENIED, False):
            return False
        current = event.get_extra(TURN)
        if current and (current.pending or current.sent):
            return True
        key = (
            str(event.get_platform_id()),
            str(event.get_self_id()),
            str(event.get_group_id()),
            str(event.get_sender_id()),
        )
        turn = self.quotas.reserve(key, self.seconds, self.maximum)
        if turn is None:
            event.set_extra(DENIED, True)
            event.stop_event()
            event.clear_result()
            logger.debug("[按ID回复限额] 本轮超出额度，已丢弃，不排队。")
            return False
        event.set_extra(TURN, turn)
        return True

    async def initialize(self):
        from astrbot.core.pipeline.process_stage.stage import ProcessStage
        from astrbot.core.pipeline.scheduler import PipelineScheduler
        from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
            AiocqhttpMessageEvent,
        )

        self.install(ProcessStage, PipelineScheduler, AiocqhttpMessageEvent)
        logger.info(
            "[按ID回复限额] 已接入3个入口；登记%d个ID，每群每ID %d秒/%d轮。",
            len(self.targets),
            self.seconds,
            self.maximum,
        )

    def install(self, process_stage, scheduler, event_class):
        if self.active:
            return
        original_process = process_stage.process
        original_execute = scheduler.execute
        original_send = event_class.send
        self.active = True

        @wraps(original_process)
        async def process(stage, event):
            if self.active and not self.admit(event):
                return
            async for value in original_process(stage, event):
                yield value

        @wraps(original_execute)
        async def execute(owner, event):
            try:
                return await original_execute(owner, event)
            finally:
                # Scheduler completion also covers stopped generators and cancellation.
                self.quotas.release(event.get_extra(TURN))

        @wraps(original_send)
        async def send(event, message):
            result = await original_send(event, message)
            # Observe actual delivery only. Other guards may silently suppress send;
            # returning normally is not by itself a successful reply.
            if (
                self.active
                and message
                and event._has_send_oper
                and any(
                    not isinstance(part, Plain) or part.text.strip()
                    for part in message.chain
                )
            ):
                self.quotas.commit(event.get_extra(TURN))
            return result

        for owner, name, original, wrapper in (
            (process_stage, "process", original_process, process),
            (scheduler, "execute", original_execute, execute),
            (event_class, "send", original_send, send),
        ):
            setattr(owner, name, wrapper)
            self.patches.append((owner, name, original, wrapper))

    async def terminate(self):
        self.active = False
        # Do not remove wrappers installed by other plugins after this one.
        for owner, name, original, wrapper in reversed(self.patches):
            if getattr(owner, name) is wrapper:
                setattr(owner, name, original)
        self.patches.clear()

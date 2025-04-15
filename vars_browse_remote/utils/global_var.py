import threading


class GlobalVar:
    _event = threading.Event()
    _lock = threading.Lock()
    _browse_var_state = True  # 是否停止遍历

    @classmethod
    def get_browse_var_state(cls):
        with cls._lock:
            return cls._browse_var_state

    @classmethod
    def set_browse_var_state(cls, value):
        with cls._lock:
            if cls._browse_var_state != value:
                cls._browse_var_state = value
                cls._event.set()  # 触发事件通知

    @classmethod
    def wait_change(cls, timeout=None):
        event_occurred = cls._event.wait(timeout)
        if event_occurred:
            cls._event.clear()  # 重置事件状态
        return event_occurred

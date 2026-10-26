"""Small macOS Keychain store for renewable local platform sessions.

Only refresh tokens are stored here. Account passwords and access tokens are
never persisted by this module.
"""
from __future__ import annotations

import ctypes
import json
import sys
from .runtime import keychain_service


_SERVICE = keychain_service()
_ACCOUNT = b"pre-athena-visual-ai-refresh"
_ERR_ITEM_NOT_FOUND = -25300
_ERR_DUPLICATE_ITEM = -25299


class SecureStoreError(RuntimeError):
    pass


def _frameworks():
    if sys.platform != "darwin":
        raise SecureStoreError("本机系统钥匙串仅支持 macOS。")
    try:
        security = ctypes.CDLL("/System/Library/Frameworks/Security.framework/Security")
        core_foundation = ctypes.CDLL("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation")
    except OSError:
        raise SecureStoreError("无法访问本机系统钥匙串。") from None
    return security, core_foundation


def _check(status: int) -> None:
    if status != 0:
        raise SecureStoreError("本机系统钥匙串操作失败。")


def _find(security):
    length = ctypes.c_uint32()
    data = ctypes.c_void_p()
    item = ctypes.c_void_p()
    security.SecKeychainFindGenericPassword.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_char_p,
        ctypes.c_uint32, ctypes.c_char_p, ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p),
    ]
    security.SecKeychainFindGenericPassword.restype = ctypes.c_int32
    status = security.SecKeychainFindGenericPassword(
        None, len(_SERVICE), _SERVICE, len(_ACCOUNT), _ACCOUNT,
        ctypes.byref(length), ctypes.byref(data), ctypes.byref(item),
    )
    if status == _ERR_ITEM_NOT_FOUND:
        return None, None, None
    _check(status)
    return length.value, data, item


def _free(security, core_foundation, data, item) -> None:
    if data:
        security.SecKeychainItemFreeContent.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        security.SecKeychainItemFreeContent.restype = ctypes.c_int32
        security.SecKeychainItemFreeContent(None, data)
    if item:
        core_foundation.CFRelease.argtypes = [ctypes.c_void_p]
        core_foundation.CFRelease.restype = None
        core_foundation.CFRelease(item)


def load_connection() -> dict[str, str] | None:
    security, core_foundation = _frameworks()
    length, data, item = _find(security)
    if length is None:
        return None
    try:
        raw = ctypes.string_at(data, length)
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict) or not isinstance(value.get("refresh"), str) or not value["refresh"]:
            return None
        user = value.get("user")
        return {"refresh": value["refresh"], "user": user if isinstance(user, str) else ""}
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SecureStoreError("本机保存的连接信息无法读取。") from None
    finally:
        _free(security, core_foundation, data, item)


def save_connection(refresh: str, user: str) -> None:
    if not isinstance(refresh, str) or not refresh:
        raise SecureStoreError("平台没有提供可续期的会话凭证。")
    security, core_foundation = _frameworks()
    payload = json.dumps({"refresh": refresh, "user": user or ""}, separators=(",", ":")).encode("utf-8")
    buffer = ctypes.create_string_buffer(payload)
    item = ctypes.c_void_p()
    security.SecKeychainAddGenericPassword.argtypes = [
        ctypes.c_void_p, ctypes.c_uint32, ctypes.c_char_p,
        ctypes.c_uint32, ctypes.c_char_p, ctypes.c_uint32,
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p),
    ]
    security.SecKeychainAddGenericPassword.restype = ctypes.c_int32
    status = security.SecKeychainAddGenericPassword(
        None, len(_SERVICE), _SERVICE, len(_ACCOUNT), _ACCOUNT,
        len(payload), ctypes.cast(buffer, ctypes.c_void_p), ctypes.byref(item),
    )
    if status == _ERR_DUPLICATE_ITEM:
        length, data, existing = _find(security)
        if length is None:
            raise SecureStoreError("无法更新本机保存的连接。")
        try:
            security.SecKeychainItemModifyAttributesAndData.argtypes = [
                ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p,
            ]
            security.SecKeychainItemModifyAttributesAndData.restype = ctypes.c_int32
            status = security.SecKeychainItemModifyAttributesAndData(
                existing, None, len(payload), ctypes.cast(buffer, ctypes.c_void_p),
            )
            _check(status)
        finally:
            _free(security, core_foundation, data, existing)
        return
    try:
        _check(status)
    finally:
        if item:
            core_foundation.CFRelease.argtypes = [ctypes.c_void_p]
            core_foundation.CFRelease.restype = None
            core_foundation.CFRelease(item)


def delete_connection() -> None:
    security, core_foundation = _frameworks()
    _, data, item = _find(security)
    if item is None:
        return
    try:
        security.SecKeychainItemDelete.argtypes = [ctypes.c_void_p]
        security.SecKeychainItemDelete.restype = ctypes.c_int32
        _check(security.SecKeychainItemDelete(item))
    finally:
        _free(security, core_foundation, data, item)

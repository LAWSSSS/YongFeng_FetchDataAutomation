import asyncio
from typing import Any, Literal

import httpx

from .domain import ORIGIN

Service = Literal["source", "target"]


class RemoteError(Exception):
    def __init__(self, message: str, code: str = "", ambiguous: bool = False):
        super().__init__(message)
        self.code = code
        self.ambiguous = ambiguous


def unwrap_response(body: Any, service: Service) -> Any:
    if not isinstance(body, dict):
        raise RemoteError("原站返回了无法识别的内容，请检查登录状态。", "RESPONSE")
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else body
    code = str(meta.get("code", body.get("code", "")))
    success = (meta.get("success") is True or body.get("success") is True) if service == "source" else str(body.get("code")) == "0"
    if not success:
        raise RemoteError(str(meta.get("message") or body.get("message") or "原站拒绝了请求。"), code)
    return body.get("data")


class RemoteClient:
    def __init__(self, service: Service, transport: httpx.AsyncBaseTransport | None = None):
        self.service = service
        segment = "srape-steel" if service == "source" else "platform"
        self.client = httpx.AsyncClient(
            base_url=f"{ORIGIN}/{segment}/api/", timeout=30,
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
            # 指定内网站点走直连，避免 HTTPX 误用 macOS 系统代理并忽略其域名例外。
            follow_redirects=False, trust_env=False, transport=transport,
        )
        self.connected = False
        self.user = ""
        self.login_lock = asyncio.Lock()

    def clear(self):
        self.client.cookies.clear()
        self.client.headers.pop("token", None)
        self.connected, self.user = False, ""

    async def request(self, path: str, *, method: str = "GET", params: dict | None = None,
                      body: Any = None, login: bool = False, write: bool = False) -> Any:
        if not login and not self.connected:
            name = "质检系统" if self.service == "source" else "采集平台"
            raise RemoteError(f"{name}尚未登录，请先连接账号。", "401")
        try:
            response = await self.client.request(method, path.lstrip("/"), params=params, **({"json": body} if body is not None else {}))
        except httpx.HTTPError:
            # 原站登录接口使用 query 传密码，禁止输出包含请求 URL 的异常内容。
            raise RemoteError("无法连接原站或请求超时，请检查网络。", "NETWORK", write) from None
        if response.status_code in (401, 403):
            self.connected = False
            raise RemoteError("登录已失效或账号无权限，请重新登录。", str(response.status_code))
        if not response.is_success:
            raise RemoteError(f"原站返回 HTTP {response.status_code}，请稍后重试。", str(response.status_code), write)
        try:
            content = response.json()
        except ValueError:
            raise RemoteError("原站返回了非 JSON 内容，请检查网站是否正常。", "RESPONSE", write) from None
        try:
            return unwrap_response(content, self.service)
        except RemoteError as error:
            if error.code in ("401", "11015"):
                self.connected = False
            raise

    async def login(self, account: str, password: str):
        async with self.login_lock:
            self.clear()
            try:
                if self.service == "source":
                    result = await self.request("/auth/login", method="POST", params={"employeeId": account.strip(), "password": password}, login=True)
                    token = (result or {}).get("tokenInfo", {}).get("tokenValue")
                    if not isinstance(token, str) or not token:
                        raise RemoteError("质检系统登录未返回有效会话，请核对账号权限。")
                    self.client.headers["token"] = token
                else:
                    await self.request("/record/doLogin", params={"account": account.strip(), "password": password}, login=True)
                self.connected = True
                await self.validate()
                self.user = account.strip()
            except RemoteError as error:
                self.clear()
                safe_message = str(error).replace(password, "[已隐藏]") if password else str(error)
                raise RemoteError(safe_message, error.code, error.ambiguous) from None
            except Exception:
                self.clear()
                raise RemoteError("登录响应格式异常，请检查原站或联系管理员。", "RESPONSE") from None

    async def validate(self):
        path = "/auth/personal_ccenter/selfInfo" if self.service == "source" else "/account/current-user"
        return await self.request(path)

    async def close(self):
        await self.client.aclose()

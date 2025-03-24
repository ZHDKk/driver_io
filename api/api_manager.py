import json
from typing import Any

import aiohttp
import requests
from logger import log


def request_post(base_url: str, req_url: str, params):
    """
    同步post请求
     api post请求：
     req_url:请求接口的路径
     data：数据
    """
    url = f"{base_url}{req_url}"
    try:
        # 发送POST请求
        response = requests.post(url, json=params)

        # 检查响应状态码
        response.raise_for_status()  # 如果状态码不是200，将引发HTTPError异常

        # 尝试解析JSON
        try:
            json_response = response.json()
            # log.info(f'request success: url:{req_url} data:{data} response:{json_response}')
            return json_response
        except json.JSONDecodeError:
            log.warning(f'解析JSON失败: url:{url} data:{params} 原始响应内容：:{response.text}')
            print("解析JSON失败，原始响应内容：", response.text)
            return None
    except requests.RequestException as e:
        # 区分网络错误和HTTP错误
        if isinstance(e.response, requests.Response) and e.response.status_code != 200:
            log.warning(f'HTTP error: {e.response.status_code} - {e.response.reason} for url={url}')
        else:
            # 处理请求过程中的任何异常（如网络问题、超时等）
            log.warning(f'http请求失败，异常信息: {e}')
        return None


def request_get(base_url: str, req_url: str, params):
    """
    同步get请求
     api get请求：
     req_url:请求接口的路径
    """
    url = f"{base_url}{req_url}"
    try:
        # 发送POST请求
        response = requests.get(url, params=params)

        # 检查响应状态码
        response.raise_for_status()  # 如果状态码不是200，这将引发HTTPError异常

        # 尝试解析JSON
        try:
            json_response = response.json()
            # log.info(f'request success: url:{req_url} data:{data} response:{json_response}')
            return json_response
        except json.JSONDecodeError:
            log.warning(f'解析JSON失败: url:{url} 原始响应内容:{response.text}')
            print("解析JSON失败，原始响应内容：", response.text)
            return None
    except requests.RequestException as e:
        # 区分网络错误和HTTP错误
        if isinstance(e.response, requests.Response) and e.response.status_code != 200:
            log.warning(f'HTTP error: {e.response.status_code} - {e.response.reason} for url={url}')
        else:
            # 处理请求过程中的任何异常（如网络问题、超时等）
            log.warning(f'http请求失败，异常信息: {e}')
        return None


async def request_post_async(base_url: str, req_url: str, params):
    """
    异步POST请求

    参数:
    base_url (str): 基础URL。
    req_url (str): 请求接口的路径。
    data (dict): 要发送的数据。

    返回:
    dict: 解析后的JSON响应，如果失败则返回None。
    """
    url = f"{base_url}{req_url}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=params) as response:
                # 检查响应状态码
                response.raise_for_status()

                # 尝试解析JSON
                try:
                    json_response = await response.json()
                    log.info(f'Request success: url={url} data={params} response={json_response}')
                    return json_response
                except json.JSONDecodeError:
                    log.warning(f'解析JSON失败:: url={url} data={params} 原始响应内容: {await response.text()}')
                    return None
        except aiohttp.ClientError as e:
            log.warning(f'http请求失败，异常信息: {e}')
            return None


async def request_get_async(base_url: str, req_url: str, params):
    """
    异步GET请求
    向API发送GET请求：
    req_url: 请求接口的路径
    """
    url = f"{base_url}{req_url}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as response:
                # 检查响应状态码
                response.raise_for_status()  # 如果状态码不是200-299，这将引发ClientError异常

                # 尝试解析JSON
                try:
                    json_response = await response.json()
                    log.info(f'Request success: url={url} response={json_response}')
                    return json_response
                except json.JSONDecodeError:
                    log.warning(f'Failed to decode JSON: url={url} raw response: {await response.text()}')
                    print("Failed to decode JSON, raw response:", await response.text())
                    return None
        except aiohttp.ClientError as e:
            log.warning(f'http请求失败，异常信息: {e}')
            return None

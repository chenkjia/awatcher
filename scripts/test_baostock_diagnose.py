#!/usr/bin/env python3
"""
BaoStock 连接诊断脚本
用于快速验证登录、基础查询与稳定性。
"""
import platform
import socket
import time

import baostock as bs


def check_dns(hostname):
    """检查主机 DNS 解析"""
    try:
        ip = socket.gethostbyname(hostname)
        return True, ip
    except Exception as e:
        return False, str(e)


def run_login_test(rounds=3, sleep_seconds=1):
    """循环登录测试，观察网络稳定性"""
    results = []
    for index in range(1, rounds + 1):
        start = time.time()
        try:
            result = bs.login(user_id="anonymous", password="123456")
            duration = time.time() - start
            ok = result.error_code == "0"
            results.append(
                {
                    "round": index,
                    "ok": ok,
                    "error_code": result.error_code,
                    "error_msg": result.error_msg,
                    "duration": round(duration, 3),
                }
            )
            if ok:
                query = bs.query_stock_basic()
                query_ok = query.error_code == "0"
                results[-1]["query_ok"] = query_ok
                results[-1]["query_error"] = query.error_msg
        except Exception as e:
            duration = time.time() - start
            results.append(
                {
                    "round": index,
                    "ok": False,
                    "error_code": "EXCEPTION",
                    "error_msg": str(e),
                    "duration": round(duration, 3),
                }
            )
        finally:
            try:
                bs.logout()
            except Exception:
                pass
        if index < rounds:
            time.sleep(sleep_seconds)
    return results


def main():
    print("=== BaoStock Diagnose ===")
    print(f"Python: {platform.python_version()}")
    print(f"Platform: {platform.platform()}")

    dns_ok, dns_info = check_dns("www.baostock.com")
    print(f"DNS(www.baostock.com): {'OK' if dns_ok else 'FAIL'} - {dns_info}")

    print("Login rounds:")
    test_results = run_login_test(rounds=5, sleep_seconds=1)
    for item in test_results:
        base = (
            f"[{item['round']}] ok={item['ok']} code={item['error_code']} "
            f"msg={item['error_msg']} duration={item['duration']}s"
        )
        if "query_ok" in item:
            base += f" query_ok={item['query_ok']} query_err={item['query_error']}"
        print(base)

    success_count = len([x for x in test_results if x["ok"]])
    print(f"Summary: {success_count}/{len(test_results)} login success")


if __name__ == "__main__":
    main()

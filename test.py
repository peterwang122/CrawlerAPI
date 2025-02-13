import requests
# 如果下面目标站不可用，请使用test.ipw.cn、ip.sb、ipinfo.io、ip-api.com、64.ipcheck.ing
targetURL = "https://ipinfo.io"
proxyAddr = "overseas.tunnel.qg.net:13871"
authKey = "NBLHO1J5"
password = "68E86C27232A"
# 账密模式
proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
    "user": authKey,
    "password": password,
    "server": proxyAddr,
}
proxies = {
    "http": proxyUrl,
    "https": proxyUrl,
}
resp = requests.get(targetURL, proxies=proxies)
print(resp.text)
import os
from wxauto import *


def init_wx():
    # 获取当前微信客户端
    wx = WeChat()
    # 获取会话列表
    wx.GetSessionList()
    # 向某人发送消息（以`文件传输助手`为例）
    msg = '你好~'
    who = '文件传输助手'
    wx.SendMsg(msg, who)  # 向`文件传输助手`发送消息：你好~


class WxBot:

    def __init__(self, who):
        self.wx = WeChat()
        self.who = who

    def send_msg(self, msg):
        self.wx.SendMsg(msg, self.who)

    def send_file(self, filepaths: list):
        abs_path = []
        for path in filepaths:
            if os.path.isabs(path) == False:
                path = os.path.join(os.getcwd(), path)
                abs_path.append(path)
            else:
                abs_path.append(path)
        self.wx.SendFiles(abs_path, self.who)


if __name__ == "__main__":
    WxBot("文件传输助手").send_file()

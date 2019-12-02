from dingtalkchatbot.chatbot import DingtalkChatbot, FeedLink, ActionCard

# WebHook地址
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=e5cc9f86eb85e5c9caef97638c6b92c36e09123c541f77289a7840071146a463'
# 初始化机器人小丁
dingding = DingtalkChatbot(webhook)

# Markdown消息@所有人
dingding.send_link(title='万万没想到，数据竟然...',
                   text='哔哩哔哩 (゜-゜)つロ 干杯~',
                   message_url='https://www.bilibili.com/',
                   pic_url='https://ss0.bdstatic.com/6KYTfyqn1Ah3otqbppnN2DJv/zheguilogo/871aacb0b25c6b25212e7db5dd89e458_originalsize.jpeg'
                   )

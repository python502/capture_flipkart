#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/9 17:27
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : send_email.py
# @Software: PyCharm
# @Desc    :
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import datetime
import os

from logger import logger
def get_delta_data(delta):
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=-delta)
    delta_time = now + delta
    delta_time = delta_time.strftime('%Y%m%d')
    return delta_time

delta_time = get_delta_data(7)

from config import *

class SendMail(object):
    @staticmethod
    def send_mail(subject, text, attachments):
        if not os.path.exists(attachments):
            raise ValueError('attachments: {} not exist'.format(attachments))
        name = os.path.basename(attachments)
        message = MIMEMultipart()
        message['From'] = Header(sender, 'utf-8')
        message['To'] = Header(','.join(receivers), 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')

        # 邮件正文内容
        message.attach(MIMEText(text, 'plain', 'utf-8'))

        # 构造附件1，传送当前目录下的 test.txt 文件
        att1 = MIMEText(open(attachments, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att1["Content-Disposition"] = 'attachment; filename="{}"'.format(name)
        message.attach(att1)
        try:
            smtpObj = smtplib.SMTP_SSL(smtp_server, 465)
            # smtpObj.set_debuglevel(1)
            smtpObj.login(sender, password)
            smtpObj.sendmail(sender, receivers, message.as_string())
            logger.info('send email success')
        except smtplib.SMTPException, ex:
            logger.error('send email fail: {}'.format(ex))
# SendMail.send_mail('review classification result email','这是评论分类结果邮件……')
import datetime
from email.mime.base import MIMEBase
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import encode_rfc2231
import os
import smtplib


def send_email_with_attachment(subject, body, to_email, from_email, smtp_server, smtp_port, login, password, file_paths):
    # 创建邮件对象
    msg = MIMEMultipart()
    msg['From'] = "SoberMH <{}>".format(from_email)  # 这里将发件人的名字设置为 "Your Name"
    msg['To'] = to_email
    msg['Subject'] = subject

    # 添加邮件正文内容
    msg.attach(MIMEText(body, 'plain'))

    # 处理附件
    for file_path in file_paths:
        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)

            # 获取文件名并进行编码
            file_name = os.path.basename(file_path)
            encoded_file_name = encode_rfc2231(file_name, 'utf-8')

            # 添加附件的 header，使用编码后的文件名
            part.add_header('Content-Disposition',
                            f"attachment; filename*={encoded_file_name}")
            msg.attach(part)

    try:
        # 连接到 SMTP 服务器
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 启用安全连接
        server.login(login, password)  # 登录 SMTP 服务器

        # 发送邮件
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()

        print(f"Email sent successfully to {to_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")


if __name__ == "__main__":
    # 使用示例
    now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    send_email_with_attachment(
        subject=f"Amazon_{now_str}",
        body="最新的亚马逊筛选数据,This email has an attachment.",
        to_email="837671287@qq.com",
        from_email="409788696@qq.com",
        smtp_server="smtp.qq.com",
        smtp_port=587,
        login="409788696@qq.com",
        password="wkevznzegbjmbhbc",
        file_paths=[r"E:\github_repositories\self_code_amazon\ae_Home_2024-10-21_18-08-23.xlsx",
                    r"E:\github_repositories\self_code_amazon\ae_Home_2024-10-21_18-08-23 copy.xlsx"]
    )

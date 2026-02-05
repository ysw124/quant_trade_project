import smtplib
from email.mime.text import MIMEText
from email.header import Header
from datetime import  datetime
from email.utils import formataddr  # 导入这个工具类

def format_to_html(intel):
    """
    将数据字典转化为带样式的 HTML 邮件模板
    """
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')

    # CSS 样式：美化表格，使其看起来更像金融终端
    html_template = f"""
    <html>
    <head>
    <style>
        body {{ font-family: 'Microsoft YaHei', sans-serif; color: #333; }}
        h2 {{ color: #004a99; border-left: 5px solid #004a99; padding-left: 10px; margin-top: 30px; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; font-size: 14px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; color: #333; }}
        tr:nth-child(even) {{ background-color: #fafafa; }}
        tr:hover {{ background-color: #f5f5f5; }}
        .timestamp {{ color: #888; font-size: 12px; }}
    </style>
    </head>
    <body>
        <h1>📊 市场全景雷达报告</h1>
        <p class="timestamp">生成时间：{now_str}</p>
        <hr>
    """

    mapping = {
        "今日潜力池": "🎯 AI + 人气共振潜力池",
        "快讯面": "📰 实时快讯 (财联社)",
        "板块面": "🔥 热门赛道 (板块行情)",
        "人气面": "📈 市场人气个股",
        "研报面": "💡 机构研报共识",
        "热搜面": "🔍 题材热搜关键词",
        "政策面": "⚖️ 政府最新政策"
    }

    for key, title in mapping.items():
        if key in intel and not intel[key].empty:
            html_template += f"<h2>{title}</h2>"
            # 使用 pandas 自带的 to_html，不显示索引，并加入 CSS 类
            table_html = intel[key].head(10).to_html(index=False, border=0, classes='table')
            html_template += table_html

    html_template += """
        <hr>
        <p style="text-align: center; color: #999;">-- 个人量化助手自动发送 --</p>
    </body>
    </html>
    """
    return html_template


def send_market_report(html_content):
    # --- 配置信息 (请替换为你的真实信息) ---
    smtp_server = "smtp.qq.com"
    sender_email = "2940538260@qq.com"
    password = "rrqcoqaeidltdhbb"  # QQ邮箱设置->账号->POP3/SMTP服务生成的16位授权码
    receiver_email = "2940538260@qq.com"
    # ------------------------------------

    message = MIMEText(html_content, 'html', 'utf-8')
    message['From'] = formataddr((Header("量化助手", 'utf-8').encode(), sender_email))
    message['To'] = formataddr((Header("交易者", 'utf-8').encode(), receiver_email))
    message['Subject'] = Header(f"【早报】市场维度情报汇总_{datetime.now().strftime('%m%d')}", 'utf-8')

    try:
        # QQ邮箱必须使用 SSL 端口 465
        with smtplib.SMTP_SSL(smtp_server, 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, [receiver_email], message.as_string())
        print("✅ 邮件简报已发送至邮箱")
    except Exception as e:
        print(f"❌ 邮件发送失败: {e}")
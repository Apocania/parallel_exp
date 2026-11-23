"""Generate a printable guide from the same lessons used by the application."""
from pathlib import Path
import html
import json

ROOT = Path(__file__).resolve().parent.parent
source = (ROOT / "web/lessons.js").read_text(encoding="utf-8")
data = json.loads(source.removeprefix("const CartLabLessons = ").strip().removesuffix(";"))
escape = html.escape
md = ["# 分布式购物车实验：从操作到原理", "", "阅读顺序：先认识系统，再按场景操作。此文中的结果是验证判据，实际是否通过要看运行后的观测。", "",
      "启动：在 E:\\parallel 运行 `python -X utf8 run.py`，打开 http://127.0.0.1:8760。", "",
      "## 先认识你正在操作的系统", ""]
body = ["<header><p class='eyebrow'>CARTLAB · 实验讲解</p><h1>分布式购物车<br>从操作到原理</h1><p>做了什么 · 验证什么 · 为什么会这样</p></header>",
        "<p>本文中的结果是验证判据；是否通过要以运行后的真实观测为准。先认识系统，再按场景操作。</p>",
        "<p>启动：在 E:\\parallel 运行 <code>python -X utf8 run.py</code>，打开 http://127.0.0.1:8760。</p>",
        "<h2 id='start'>先认识系统</h2>"]

def paragraphs(values):
    for text in values:
        md.extend([text, ""])
        body.append("<p>"+escape(text)+"</p>")

def section(title, values):
    md.extend(["### "+title, ""])
    body.append("<h3>"+escape(title)+"</h3><ul>")
    for value in values:
        md.append("- "+value)
        body.append("<li>"+escape(value)+"</li>")
    md.append("")
    body.append("</ul>")

paragraphs(data["intro"])
md.extend(["## 怎么读界面", ""])
body.append("<h2 id='screen'>怎么读界面</h2>")
paragraphs(data["reading"])
for i, lesson in enumerate(data["lessons"], 1):
    title=f"{i:02d} · {lesson['title']}"
    md.extend(["## "+title, "", "**要解决的问题：**"+lesson["question"], "",
               "**验证目标：**"+lesson["goal"], "", "**初始状态：**"+lesson["setup"], "", "### 操作、观察与解释", ""])
    body.append(f"<section id='{lesson['id']}' class='experiment'><h2>{escape(title)}</h2>"
                f"<p class='question'>{escape(lesson['question'])}</p><h3>验证目标</h3><p>{escape(lesson['goal'])}</p>"
                f"<h3>初始状态</h3><p>{escape(lesson['setup'])}</p><h3>操作、观察与解释</h3>")
    for n, step in enumerate(lesson["steps"], 1):
        md.extend([f"**第 {n} 步：{step['do']}**", "", "- 应该看到："+step["see"], "- 为什么："+step["why"], ""])
        body.append(f"<div class='step'><h4>第 {n} 步 · {escape(step['do'])}</h4>"
                    f"<p><b>应该看到：</b>{escape(step['see'])}</p><p><b>为什么：</b>{escape(step['why'])}</p></div>")
    section("用到的原理", lesson["principles"])
    section("通过判据", lesson["success"])
    section("边界与常见误解", lesson["limits"])
    md.extend(["### 展示时可以这样讲", "", "> "+lesson["say"], ""])
    body.append("<h3>展示时可以这样讲</h3><blockquote>"+escape(lesson["say"])+"</blockquote>")
    for faq in lesson["faq"]:
        md.extend(["**"+faq["q"]+"**", "", faq["a"], ""])
        body.append("<h4>"+escape(faq["q"])+"</h4><p>"+escape(faq["a"])+"</p>")
    body.append("</section>")
md.extend(["## 术语速查", "", "| 术语 | 通俗解释 |", "|---|---|"])
body.append("<h2 id='terms'>术语速查</h2><table><thead><tr><th>术语</th><th>通俗解释</th></tr></thead><tbody>")
for name, meaning in data["terms"]:
    md.append(f"| {name} | {meaning} |")
    body.append("<tr><td>"+escape(name)+"</td><td>"+escape(meaning)+"</td></tr>")
body.append("</tbody></table><h2 id='closing'>怎么完成一次清楚的展示</h2>")
md.extend(["", "## 怎么完成一次清楚的展示", ""])
paragraphs(data["closing"])
nav = "<a href='#start'>先认识系统</a><a href='#screen'>怎么读界面</a>"+"".join(
    f"<a href='#{l['id']}'>{i:02d} {escape(l['title'].split('：')[0])}</a>"
    for i, l in enumerate(data["lessons"], 1))+"<a href='#terms'>术语速查</a>"
css = """
body{margin:0;font:16px/1.9 'Segoe UI','Microsoft YaHei',sans-serif;color:#253c35;background:#f4f7f3}
nav{position:fixed;width:205px;height:100vh;overflow:auto;padding:24px;box-sizing:border-box;background:#18332e}
nav a{display:block;color:#dcebe1;text-decoration:none;font-size:13px;padding:6px 0}main{max-width:920px;margin:0 auto 0 240px;padding:35px 40px 80px}
header{border-bottom:3px solid #278469;margin-bottom:30px;padding-bottom:25px}h1{font-size:38px;line-height:1.35}
.eyebrow{color:#287d64;letter-spacing:3px;font-size:12px}h2{font-size:25px;margin-top:50px;line-height:1.5;scroll-margin:20px}
h3{font-size:19px;margin:26px 0 9px}h4{font-size:16px;margin:12px 0}p{margin:10px 0}li{margin:7px 0}
.question,blockquote{background:#e5f1e8;border-left:4px solid #3c9272;padding:16px 20px;margin:20px 0}
.step{background:#fff;border:1px solid #dce7dd;border-radius:8px;padding:15px 20px;margin:12px 0}
table{border-collapse:collapse;width:100%;font-size:14px}th,td{border:1px solid #d5dfd7;text-align:left;padding:12px;vertical-align:top}th{background:#e7f0e8}
code{background:#e9eeea;padding:3px 6px;overflow-wrap:anywhere}.experiment{border-top:1px solid #ccdcd0;margin-top:35px}
@media(max-width:850px){nav{position:static;width:auto;height:auto;display:flex;flex-wrap:wrap;gap:8px 16px;padding:18px}main{margin:0;padding:24px}h1{font-size:30px}}
@media print{nav{display:none}body{background:white;color:#111;font-size:11pt}main{margin:0;padding:0;max-width:none}h2{break-before:page;margin-top:0}.step,blockquote,tr{break-inside:avoid}h3,h4{break-after:avoid}a{color:inherit}.experiment{border:0}.question{background:#f4f4f4}@page{size:A4;margin:18mm}}
"""
(ROOT/"docs/实验讲解详解.md").write_text("\n".join(md), encoding="utf-8")
(ROOT/"docs/实验讲解详解.html").write_text(
    "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
    "<title>CartLab · 实验讲解详解</title><style>"+css+"</style></head><body><nav>"+nav+"</nav><main>"+"\n".join(body)+"</main></body></html>",
    encoding="utf-8")
print(f"Generated {len(data['lessons'])} lessons and {len(data['terms'])} glossary entries.")

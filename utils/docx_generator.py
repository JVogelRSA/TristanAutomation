import io
from datetime import datetime
from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from htmldocx import HtmlToDocx


def html_to_docx(html_content, title, date_str=None):
    """
    Convert LLM-generated HTML report into a DOCX file.
    Returns DOCX bytes ready for email attachment.
    """
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    doc = Document()

    # Set narrow margins for compact layout (1-2 pages)
    for section in doc.sections:
        section.top_margin = Inches(0.6)
        section.bottom_margin = Inches(0.6)
        section.left_margin = Inches(0.7)
        section.right_margin = Inches(0.7)

    # Title
    title_para = doc.add_paragraph()
    title_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title_para.add_run(title)
    run.bold = True
    run.font.size = Pt(16)

    # Date subtitle
    date_para = doc.add_paragraph()
    date_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = date_para.add_run(date_str)
    run.font.size = Pt(10)
    run.font.color.rgb = None  # default color

    # Divider
    doc.add_paragraph('─' * 60)

    # Convert HTML body to DOCX elements
    parser = HtmlToDocx()
    parser.table_style = 'Light Grid Accent 1'

    # Clean up common LLM output artifacts
    html_content = html_content.strip()
    if html_content.startswith('```html'):
        html_content = html_content[7:]
    if html_content.endswith('```'):
        html_content = html_content[:-3]
    html_content = html_content.strip()

    parser.add_html_to_document(html_content, doc)

    # Compact all paragraph fonts to keep report tight
    for para in doc.paragraphs:
        for run in para.runs:
            if run.font.size is None or run.font.size > Pt(11):
                run.font.size = Pt(10)

    # Write to bytes
    buffer = io.BytesIO()
    doc.save(buffer)
    return buffer.getvalue()

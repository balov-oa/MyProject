from openpyxl.styles import (
    Alignment,
    Font,
    NamedStyle,
    PatternFill,
)

font = Font(
    color='FF928B',
    b=True,
    i=True,
    name='SegoeUI',
    size=11
)

alignment = Alignment(
    horizontal='center',
    vertical='center',
    wrap_text=True,
    shrink_to_fit=False,
    indent=0
)

headers_style = NamedStyle(name="headers_style")
headers_style.font = Font(b=True, name='SegoeUI', size=11)
headers_style.fill = PatternFill(fgColor='CDEAC0', fill_type='solid')
headers_style.alignment = Alignment(
    horizontal='center',
    vertical='center',
    wrap_text=True,
    shrink_to_fit=False,
    indent=0
)

base_style = NamedStyle(name="base_style")
base_style.font = Font(name='SegoeUI', size=10)
base_style.alignment = Alignment(horizontal='center', vertical='center')
text_wrap_style = NamedStyle(name="text_wrap_style")
text_wrap_style.alignment = Alignment(
    horizontal='center',
    vertical='center',
    wrap_text=True,
    shrink_to_fit=True,
    indent=0
)

import os, re, glob

for path in glob.glob('src/**/*.astro', recursive=True):
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace window[\'something\'] with window["something"]
    new_content = re.sub(r"window\[\\'([a-zA-Z0-9_]+)\\'\]", r'window["\1"]', content)
    
    if new_content != content:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print('Updated', path)

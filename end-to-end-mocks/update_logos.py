import re

files = [
    '02-ai-analysis-processing.html',
    '03-analysis-results.html',
    '04-sales-upload.html',
    '05-ai-matching-progress.html',
    '06-royalty-dashboard.html',
    '07-invoice-generation.html',
    '08-rag-qna.html'
]

old_pattern = '''            <div class="p-6 border-b border-border">
                <h1 class="text-2xl font-bold text-blue-600">License IQ</h1>
                <p class="text-sm text-gray-600 mt-1">Research Platform</p>
            </div>'''

new_pattern = '''            <div class="p-6 border-b border-border">
                <div class="flex items-center mb-2">
                    <div class="w-10 h-10 bg-gradient-to-br from-blue-500 to-indigo-600 rounded-lg flex items-center justify-center mr-3">
                        <span class="text-xl font-bold text-white">C</span>
                    </div>
                    <h1 class="text-2xl font-bold text-blue-600">License IQ</h1>
                </div>
                <p class="text-xs text-gray-500">by CimpleIT Platform</p>
            </div>'''

for filename in files:
    with open(filename, 'r') as f:
        content = f.read()
    
    content = content.replace(old_pattern, new_pattern)
    
    with open(filename, 'w') as f:
        f.write(content)
    
    print(f"Updated {filename}")

print("All files updated successfully!")

with open('acis.log', 'r', encoding='utf-8') as f:
    for line in f:
        if 'customer' in line.lower() and 'generator' in line.lower():
            print(line.strip())

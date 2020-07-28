import sys
import os

if __name__ == '__main__':
    lst = []
    for file in os.listdir(sys.argv[1]):
        if not file.startswith('part'):
            continue
        file = os.path.join(sys.argv[1], file)
        with open(file, encoding='utf-8') as fp:
            lst.extend(fp.readlines())
    with open('nodes.csv', 'w', encoding='utf-8') as fp:
        fp.write('Id,Label,书名\n')
        for line in lst:
            name, book = line.split('\t')
            fp.write(','.join([name, name, book]))
    s = ""
    for file in os.listdir(sys.argv[2]):
        if not file.startswith('part'):
            continue
        file = os.path.join(sys.argv[2], file)
        with open(file, encoding='utf-8') as fp:
            s += fp.read()
    s = s.replace('<', '')
    s = s.replace('>', '')
    s = s.replace('\t', ',Undirected,')
    with open('edges.csv', 'w', encoding='utf-8') as fp:
        fp.write('Source,Target,Type,Weight\n')
        fp.write(s)
    """
    d = {}
    i = 0
    for line in fp.readlines():
        name, _ = line.split('\t')
        d[name] = str(i)
        i += 1
    with open('temp.csv', encoding='utf-8') as fp1, open('edges.csv', 'w', encoding='utf-8') as fp2:
        fp2.write('Source,Target,Weight\n')
        for line in fp1.readlines():
            source, target, weight = line.split(',')
            source = d[source]
            target = d[target]
            fp2.write(','.join([source, target, weight]))
    """
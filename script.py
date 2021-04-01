import sys

if len(sys.argv) != 2:
    sys.exit()

with open(sys.argv[1]) as f:
    for line in f:
        nodes = line.split()
        if nodes[0] == nodes[1]:
            continue
        print("\t".join(sorted(nodes)))
#!/usr/bin/env python

from hashring import HashRing
import sys

try:
    import numpy
except ImportError:
    numpy = None

ring = None
nodes = {}

def read_values(path):
    with open(path, 'r') as fd:
        return [s.strip() for s in fd.readlines()]

def create_ring(replicas):
    global ring
    ring = HashRing(nodes.keys(), replicas)

def distribute_keys(keys):
    for key in keys:
        nodes[ring.get_node(key)] += 1

def usage():
    print """usage: %s <path to keys> <path to hosts> <number of replicas>

Host file and key file should be newline separated lists of values.""" % sys.argv[0]

if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
        sys.exit()

    keyfile = sys.argv[1]
    hostfile = sys.argv[2]
    replicas = int(sys.argv[3])

    keys = read_values(keyfile)
    for host in read_values(hostfile):
        nodes[host] = 0

    create_ring(replicas)
    distribute_keys(keys)

    for node, num_keys in nodes.iteritems():
        print "%s: %d" % (node, num_keys)

    if numpy:
        a = numpy.array(nodes.values())
        print "variance: %f, stddev: %f" % (a.var(), a.std())

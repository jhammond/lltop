import os
import sys
import operator
from lxml import objectify

LLTOPCMD = "/home/royd/projects/git/lltop/lltop --lltop-serv=/root/lltop-serv --job-map=/bin/true -i 10"
OSSLIST = "10.1.3.1,10.1.3.11,10.1.3.12,10.1.3.13,10.1.3.14".split(",")
TOPLINES = 10


def pbsnodes():
    pbsnodesxml = os.popen('pbsnodes -x', 'r')
    pbsnodesdata = objectify.parse(pbsnodesxml).getroot()
    nodes = dict()
    for n in pbsnodesdata.iterchildren(tag='Node'):
        nodename = str(n['name'])
        nodes[nodename] = n
    return nodes

def qstat():
    qstatxml = os.popen('qstat -t -f -x', 'r') 
    qstatdata = objectify.parse(qstatxml).getroot()
    jobs = dict()
    for j in qstatdata.iterchildren(tag='Job'):
        jobid = str(j['Job_Id'])
        jobs[jobid] = j
    return jobs

def lltop(hostlist):
    lltop = os.popen(" ".join([LLTOPCMD, "-l"] + hostlist)).readlines()
    #lltop = file("/tmp/lltop.txt", "r").readlines()
    lltopdata = list()
    for line in lltop[1:]:
        items = line.strip().split()
        lltopdata.append([items[0]] + map(int, items[1:]))
    return lltopdata

def nodejobs(status):
    for s in status.split(","):
        if s.startswith("jobs="):
            joblist = s.split("=", 1)[1]
            return joblist.split(" ")

def lustre_to_torque(clientname):
    # map from ib network hostname to torque nodelist hostname
    # our cluster has cX-Y as torque nodename with cibX-Y
    # as lustre client hostname (we run lustre over infiniband)
    return clientname.replace("ib", "")

def printstats(lltopdata, nodeinfo, joblist):
    print "host          write MB    read MB     reqs   jobid(user)"
    for llt in lltopdata[:TOPLINES]:
        print "%10s %10s %10s %10s" % tuple(llt),
        hostname = lustre_to_torque(llt[0])
        jobs = nodejobs(str(nodeinfo[hostname].status))
        for j in jobs:
            jobstats = joblist[j]
            print "   %s(%s)" % (j.split(".")[0],
                              str(jobstats.Job_Owner).split("@")[0]),
        print

def printsummary(lltopdata):
    total_writemb = sum(map(operator.itemgetter(1), lltopdata))
    total_readmb = sum(map(operator.itemgetter(2), lltopdata))
    total_reqs = sum(map(operator.itemgetter(3), lltopdata))
    print "Total: writes %s MB, reads %s MB, iops %s" % (total_writemb, total_readmb, total_reqs)

def main():
    lltopdata = lltop(OSSLIST)
    nodeinfo = pbsnodes()
    joblist = qstat()

    writecol = lambda x: x[1]
    readcol = lambda x: x[2]
    iopcol = lambda x: x[3]
    print "top writers"
    lltopdata.sort(key=writecol, reverse=True)
    printstats(lltopdata, nodeinfo, joblist)
    print "top readers"
    lltopdata.sort(key=readcol, reverse=True)
    printstats(lltopdata, nodeinfo, joblist)
    print "top iops"
    lltopdata.sort(key=iopcol, reverse=True)
    printstats(lltopdata, nodeinfo, joblist)

    printsummary(lltopdata)

if __name__ == "__main__":
    main()
    

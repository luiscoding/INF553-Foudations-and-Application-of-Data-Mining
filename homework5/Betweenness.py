
from pyspark import SparkContext
import math
import sys, os

thre = 3
path = sys.argv[1]
output = sys.argv[2]

sc = SparkContext("local", "Simple App")

class Graph:
    B = {}
    vertices = {} 

    def add_edge(self, u, v):
        if u not in self.vertices:
            self.vertices[u]=list()
        if v not in self.vertices:
            self.vertices[v]=list()
        self.vertices[u].append(v)
        self.vertices[v].append(u)

 
    def bfs(self, root): 
        q = list()          # store the nodes to search
        visit = dict()      # mark visited nodes
        distance = dict()   # store the level of nodes (distances to root)
        up = dict()         # store the father node of each node

        q.append(root)
        distance[root] = 0
        visit[root] = True
        for u in self.vertices[root]:
            up[u] = [root,]
            distance[u] = 1
            visit[u]=True
            q.append(u)

        index = 1
        while index<len(q):
            u = q[index]
            index+=1
            visit[u] = True
            for v in self.vertices[u]:
                if v not in visit:
                    if v not in distance:
                        q.append(v)
                        distance[v] = distance[u]+1
                        up[v] = [u,]
                    else:
                        if distance[v]>distance[u]:
                            distance[v] = distance[u]+1
                            up[v].append(u)
        self.calcB(q, distance, up)
    def calcB(self, invert, distance, up):
        hold = dict()
        for key in reversed(invert[1:]):
            if key not in hold:
                hold[key]=1
            if len(up[key])==0:
                continue
            rate = float(1)/len(up[key])
            for i in up[key]:
                if i<key:
                    my_tuple = (i, key)
                    if my_tuple not in self.B:
                        self.B[my_tuple]=rate*hold[key]
                    else:
                        self.B[my_tuple]+=rate*hold[key]
                if i not in hold:
                    hold[i] = 1
                hold[i] += (rate*hold[key])

    def trav(self):
        for root in self.vertices:
            self.bfs(root)
            
def get_same(x):
    s1 = set(x[0][1])
    s2 = set(x[1][1])
    same = len(s1.intersection(s2))
    if same>=thre:     
        return True
    else:
        return False
 
def write(bet):
    fo = open(output, "w+")
    for k in sorted(bet.iterkeys()):
        thisline = "("+str(k[0])+","+str(k[1])+","+str(bet[k])+")\n"
        fo.write(thisline)
    fo.close()
    return
ori = sc.textFile(path).filter(lambda x: not("userid" in x.lower())).map(lambda k: k.split(",")).map(lambda x: ( int(x[0]), int(x[1]) ) )
users = ori.groupByKey().map(lambda (x,y): (x,sorted(list(y)))).sortByKey() # ((user1,[m1, m3, m7, m12...]), (u2, [...]), (...) ... )
user_temp = users.map(lambda x: (1, x))
user_pair = user_temp.join(user_temp).map(lambda x: x[1]).filter(lambda (x, y): x[0]<y[0]) #(  ( (u1, [...]), (u2, [...]) ), ( (),  () )  )
user_pair = user_pair.filter(lambda x: get_same(x)).map(lambda x: (x[0][0],x[1][0])) # ( (1,4), (1,8), (1, 12), ...)

g = Graph()
edge = list(user_pair.collect())
for e in edge:
    g.add_edge(e[0], e[1])
g.trav()
write(g.B)
sc.stop()




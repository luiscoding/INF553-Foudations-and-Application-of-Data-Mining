
from pyspark import SparkContext
import math
import sys, os

thre = 3
path = sys.argv[1]
output = sys.argv[2]

sc = SparkContext("local", "Simple App")

# ========= Graph class ==========
class Graph:
    B = {}          # store the betweenness
    vertices = {}   # store the vertices and their neighbors
    Aij = {}        # adjacency matrix
    Bij = {}        # Aij - (Ki*Kj/2m)
    m = 0           # number of edges
    belongsto = {}  # store which group (community) each node belongs to

    def __init__(self, edges):
        self.m = len(edges)
        for e in edges:
            self.add_edge(e[0], e[1])
        n = len(self.vertices)
        for i in range(1, n+1):
            self.Aij[i] = dict()
            for j in range(1, n+1):
                self.Aij[i][j] = 0
        for u in self.vertices:
            for v in self.vertices[u]:
                self.Aij[u][v] = 1

    def initBelong(self):
        for u in self.vertices:
            self.belongsto[u] = 1

    def add_edge(self, u, v):
        if u not in self.vertices:
            self.vertices[u]=list()
        if v not in self.vertices:
            self.vertices[v]=list()
        self.vertices[u].append(v)
        self.vertices[v].append(u)
         
    def degree(self, node):
        return len(self.vertices[node])

    def calcMatrix(self):
        for u in self.vertices:
            if u not in self.Bij:
                self.Bij[u] = dict()
            for v in self.vertices:
                self.Bij[u][v] = self.Aij[u][v]-float(self.degree(u)*self.degree(v))/(2*self.m)

    def divide(self, e):
        # run psudo-BFS from both end-node of the edge:
        for i in range(0,2):
            root = e[i]
            visit = dict()
            q = list()
            visit[root]=True
            q.append(root)

            index = 0
            while index<len(q):
                u = q[index]
                index+=1
                for v in self.vertices[u]:
                    if self.Aij[u][v]==1:
                        if v == e[1-i]:
                            return False    # if find the other end node: no new components
                        if v not in visit:
                            q.append(v)
                            self.belongsto[v] = self.belongsto[root]
                            visit[v] = True
            self.belongsto[e[1]]=e[1]
        return True

    def bfs(self, root): 
        q = list()
        visit = dict()
        distance = dict()
        up = dict()

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

def write(mylist):
    fo = open(output, "w+")
    for k in mylist:
        k = sorted(k)
        thisline = "["
        for x in k:
            thisline += (str(x)+",")
        thisline = thisline[:-1]
        thisline+="]\n"
        fo.write(thisline)
    fo.close()
    return

ori = sc.textFile(path).filter(lambda x: not("userid" in x.lower())).map(lambda k: k.split(",")).map(lambda x: ( int(x[0]), int(x[1]) ) )
users = ori.groupByKey().map(lambda (x,y): (x,sorted(list(y)))).sortByKey() # ((user1,[m1, m3, m7, m12...]), (u2, [...]), (...) ... )

user_temp = users.map(lambda x: (1, x))
user_pair = user_temp.join(user_temp).map(lambda x: x[1]).filter(lambda (x, y): x[0]<y[0]) #(  ( (u1, [...]), (u2, [...]) ), ( (),  () )  )
user_pair = user_pair.filter(lambda x: get_same(x)).map(lambda x: (x[0][0],x[1][0])) # ( (1,4), (1,8), (1, 12), ...)

edge = list(user_pair.collect())
g = Graph(edge)
g.trav()
                            
g.calcMatrix()
# sort edges by betweenness:
remove_order = sorted(g.B, key=lambda k: g.B[k], reverse=True)
down = 2*len(g.B)
iniM = 0
for u in g.vertices:
    for v in g.vertices:
        iniM += g.Bij[u][v]
maxM = iniM/down
maxbelong = dict()
M = 0
g.initBelong()

for e in remove_order:
    g.Aij[e[0]][e[1]] = 0
    g.Aij[e[1]][e[0]] = 0
    if g.divide(e)==True:
        M=0
        for u in g.vertices:
            for v in g.vertices:
                if g.belongsto[u]==g.belongsto[v]:  # u v in the same group
                    M+=g.Bij[u][v]
        M = M/down

        if M>maxM:
            maxM = M
            maxbelong = g.belongsto.copy()
        elif M<maxM:
            continue

groups = dict()
for (node, grp) in maxbelong.items():
    if grp not in groups:
        groups[grp] = list()
    groups[grp].append(node)

community = sorted([v for (k,v) in groups.items()])

write(community)
sc.stop()




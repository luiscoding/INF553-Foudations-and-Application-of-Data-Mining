from pyspark import SparkContext

from collections import defaultdict
import math

def pearson_correlation(profile1,profile2,weight=False):
	corated = [i for i in profile1 if i in profile2]
	if len(corated)==0:
		return 0
	avg_co_rating_1 = sum([profile1[j] for j in corated])/len(corated)
	avg_co_rating_2 = sum([profile2[j] for j in corated])/len(corated)
	numerator = sum([(profile1[i]-avg_co_rating_1)*(profile2[i]-avg_co_rating_2) for i in corated])
	denominator = math.sqrt(sum([(profile1[i]-avg_co_rating_1)**2for i in corated]))*math.sqrt(sum([(profile2[i]-avg_co_rating_2)**2for i in corated]))
	if denominator == 0:
		return 0
	return numerator/denominator

def cosine_similarity(profile1,profile2):
	corated = [i for i in profile1 if i in profile2]
	if len(corated) == 0:
		return 0
	product = sum([profile1[i]*profile2[i] for i in corated])
	norm1 = math.sqrt(sum([profile1[j] for j in profile1]))
	norm2 = math.sqrt(sum([profile2[j] for j in profile2]))
	den = norm1*norm2
	if den == 0:
		return -1

	return float(product)/den

def jaccard_similarity(profile1,profile2):
	common = sum([1 for i in profile1 if i in profile2])
	Union = len(profile1)+len(profile2) - common
	if Union == 0:
		return -1
	return float(common)/Union



def getNearestNeighborsmovie(target,similarity,nNeighbors=None):
	similarities = [(similarity(movie_profiles[target],movie_profiles[other]),other) for other in movie_profiles if target!=other]
	
	similarities.sort(reverse = True)

	similarities = list(filter(lambda line:line[0]!=0,similarities))
	
	if nNeighbors!= None:
		similarities = similarities[0:nNeighbors]
	
	return similarities

def getNearestNeighborsusers(target,similarity,nNeighbors=None):
	similarities = [(similarity(user_profiles[target],user_profiles[other]),other) for other in user_profiles if target!=other]
	
	similarities.sort(reverse = True)

	similarities = list(filter(lambda line:line[0]!=0,similarities))
	
	if nNeighbors!= None:
		similarities = similarities[0:nNeighbors]
	
	return similarities

def buildmovieModel(profiles,similarity,nNeighbors = None, pathDump=None):
	model ={}
	for item in profiles:
		model.setdefault(item,{})
		correlations = getNearestNeighborsmovie(item,similarity,nNeighbors)

		for  correlation,neighbour in correlations:
			model[item][neighbour] = correlation
	for c in model:
		COLSUM = sum([model[c][r] for r in model[c]])
		if COLSUM > 0:
			for r in model[c]:
				model[c][r] /= COLSUM

	if pathDump!=None:
		dumpModel(model,pathDump)
	print("Movie Model is Complete")
	return model

def builduserModel(profiles,similarity,nNeighbors = None, pathDump=None):
	model ={}
	for user in profiles:
		model[user] = getNearestNeighborsusers(user,similarity,nNeighbors)


	if pathDump!=None:
		dumpModel(model,pathDump)
	print("User Model is Complete")
	return model

#address cold start problem
def new_movie_predictive_ratings(user,movie):  
	if user_profiles[user]:
		avg_rating = sum([user_profiles[user][m] for m in user_profiles[user]])/len(user_profiles[user])
		
		return avg_rating
	else :
		
		return -1

def predictive_ratings(user,movie,model):
	try:
		co_rated_movies = [m for m in model[movie] if m in user_profiles[user]]
		if len(co_rated_movies)!=0 :
			predicted_rating = sum([model[movie][m]*user_profiles[user][m] for m in co_rated_movies])/sum([abs(model[movie][m])for m in co_rated_movies])
			return abs(predicted_rating)
		else:
			return new_movie_predictive_ratings(user,movie)
	except KeyError:
		predicted_rating = new_movie_predictive_ratings(user,movie)
		return predicted_rating





sc =  SparkContext()
sc.setLogLevel("ERROR") 
train = sc.textFile("ml-latest-small/ratings.csv")
test = sc.textFile("testing_small.csv")
header = train.first()
train = train.filter(lambda line : line!=header).persist()
header = test.first()
test_users = test.filter(lambda line : line!=header).map(lambda line : line.split(",")).map(lambda line:(int(line[0]),int(line[1]))).persist()
actual_users = train.map(lambda line : line.split(",")).map(lambda line : (int(line[0]),int(line[1])))
train_users = actual_users.subtract(test_users).map(lambda (user,movie):((user,movie),1))
main_ratings = train.map(lambda line : line.split(",")).map(lambda line : ((int(line[0]),int(line[1])),float(line[2])))
train_ratings = main_ratings.join(train_users).map(lambda ((user,movie),(r1,r2)):(user,movie,r1))

user_profiles = defaultdict(dict)
movie_profiles = defaultdict(dict)

for (user,movie,ratings) in train_ratings.collect():
	user_profiles[user][movie] = ratings
	movie_profiles[movie][user] = ratings
#future approach for hybrid system
#user_model = builduserModel(user_profiles,pearson_correlation)
movie_model = buildmovieModel(movie_profiles,pearson_correlation,3)
predicted_rating =defaultdict(dict)
for user,movie in test_users.collect():
	rate =predictive_ratings(user,movie,movie_model)
	if (rate>=0 and rate<=5):
		predicted_rating[user][movie] = rate
	else:
		print("Outlier at" ,user,movie,rate)
		predicted_rating[user][movie] = rate

header = sc.parallelize(["User_Id,Movie_Id,Pred_Ratings"])
predicted_rdd=test_users.map(lambda (user,movie):((user,movie),predicted_rating[user][movie]))
actual_diff = main_ratings.join(predicted_rdd).map(lambda ((user,movie),(r1,r2)):abs(r1-r2))
predicted_rdd=predicted_rdd.map(lambda ((user,movie),ratings): (str(user)+","+str(movie)+","+str(ratings)))
sc.parallelize(["User_Id,Movie_Id,Pred_Ratings"]).union(predicted_rdd).coalesce(1).saveAsTextFile("Priyambada_Jain_Result_Task2.txt")

MSE = actual_diff.map(lambda line : line**2).mean()
RMSE = math.sqrt(MSE);
range1 = actual_diff.filter(lambda (rate): rate>=0 and rate<1).count()
range2 = actual_diff.filter(lambda (rate): rate>=1 and rate<2).count()
range3 = actual_diff.filter(lambda (rate): rate>=2 and rate<3).count()
range4 = actual_diff.filter(lambda (rate): rate>=3 and rate<4).count()
range5 = actual_diff.filter(lambda (rate): rate>=4).count()

print(">=0 and <1:",range1)
print(">=1 and <2:",range2)
print(">=2 and <3:",range3)
print(">=3 and <4:",range4)
print(">=4 :",range5)

print("RMSE :",RMSE)


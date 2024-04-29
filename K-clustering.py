import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

data=pd.read_csv("Iris.csv")
X=data.iloc[:,1:5].values
scaler=StandardScaler()
X_std=scaler.fit_transform(X)

from sklearn.cluster import KMeans
wcss=[]
for i in range(1,15):
    kmeans=KMeans(n_clusters=i, n_init=10)
    kmeans.fit(X_std)
    wcss.append(kmeans.inertia_)
    
plt.plot(range(1,15),wcss,'-bx')
plt.title('Elbow Method')
plt.xlabel('No. of clusters')
plt.ylabel('WCSS')
plt.show()

kmeans=KMeans(n_clusters=3)
kmeans.fit(X_std)
print(kmeans.cluster_centers_)
print(kmeans.labels_)

plt.scatter(data['SepalLengthCm'], data['SepalWidthCm'],c=kmeans.labels_)
plt.show()
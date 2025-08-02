#!/usr/bin/env python
# coding=utf-8
from numpy import array
from pandas import concat,DataFrame,Series
class ClusterPca:
    '''
    1.Hierarchical Clustering.
    2.Principal Component Analysis.
    '''
    def __init__(self):
        # input data of pandas.DataFrame.
        self.raw_df=None 

        # input DataFrame transformed into matrix and its columns.
        self.mat=None
        self.columns=None

        # matrix of eigen vectors, and colums.
        self.cov_matrix=None
        self.eigen_vector_matrix=None
        self.eigen_values=None
        self.principal_matrix=None

        # main part of final data.
        self.principal_data=None
        self.p_col=None 

        # other part of the final data.
        self.other_data=None
        self.other_col=None
        
        # current data freshed at any time.
        self.cur_data=None # current data is pandas.DataFrame.
        pass
    def load_matrix(self,indf):
        '''
        Input data can be pandas.DataFrame or numpy.ndarray.
        '''
        self.raw_df=indf.fillna(0.0)
        self.mat=self.raw_df.values
        self.columns=list(self.raw_df.columns)
        self.fresh()
        pass
    def fresh(self):
        if self.mat is not None and self.columns is not None and self.mat.shape[1]==len(self.columns):
            self.cur_data=DataFrame(self.mat,columns=self.columns)
            print('Current data freshed!')
        else:
            pass
    def clear_data(self):
        # matrix of eigen vectors, and colums.
        self.cov_matrix=None
        self.eigen_vector_matrix=None
        self.eigen_values=None
        self.principal_matrix=None
        # main part of final data.
        self.principal_data=None
        self.p_col=None 
        # other part of the final data.
        self.other_data=None
        self.other_col=None
        # current data freshed at any time.
        self.cur_data=None # current data is pandas.DataFrame.
        pass
    def standardize(self):
        from sklearn import preprocessing
        self.mat=preprocessing.scale(self.mat,axis=0)
        self.fresh()
        pass
    def hie_cluster(self,method='average',metric='minkowski',no_plot=False):
        from scipy.cluster.hierarchy import linkage,dendrogram
        z=linkage(self.mat,method=method,metric=metric)
        den=Series(dendrogram(z,orientation='right',show_leaf_counts=True,no_plot=no_plot))
        leaf=den['leaves']
        print("Get `dendrogram`(聚类树图):")
        print(den)
        return [z,leaf,den]
    def cov_pca(self):
        from numpy.linalg import eigh,inv
        from numpy import cov,dot
        cov_mat=cov(self.mat,rowvar=False) # covariance matrix 协方差矩阵，每列是一个向量。
        self.cov_matrix=cov_mat
        # characteristic value(eigenvalue 特征值), or singular value(奇异值).
        eigen_values,eigen_vector=eigh(cov_mat)
        # Covariance Matrix must be Symmetric Matrix, so we choose numpy.linalg.eigh instead of numpy.linalg.eig.协方差矩阵是对称矩阵(转制等于逆)，必然可对角化，直接采用eigh方法更快，无需使用eig方法。
        eigen_values=Series(eigen_values,index=self.columns)
        self.eigen_values=eigen_values # update attribute before sorting.
        eigen_values_sort=eigen_values.sort_values(ascending=False).round(6) # ascending 升序
        eigen_vector=DataFrame(eigen_vector,columns=self.columns) # 特征向量矩阵
        self.eigen_vector_matrix=eigen_vector
        proportion_of_variance=eigen_values_sort/eigen_values_sort.sum() # Proportion of Variance 方差贡献率
        cum_proportion=proportion_of_variance.cumsum() # Cumulative Proportion 累计方差贡献率
        cum_proportion=DataFrame(cum_proportion,columns=['cum_proportion'])
        eigen_values_selected=eigen_values_sort[cum_proportion[cum_proportion['cum_proportion']<0.92].index]
        self.principal_matrix=dot(self.mat,eigen_vector)
        # self.principal_matrix=dot(inv(eigen_vector),self.mat)
        # self.principal_matrix=dot(self.principal_matrix,eigen_vector)
        print('Cumulative Proportion of Variance:')
        print((eigen_values_selected.cumsum()/eigen_values_selected.sum()).round(3))
        print('Eigen Value Selected:')
        print(eigen_values_selected)
        print('Principal Component Matrix:')
        print(self.principal_matrix)
        self.fresh()
        pass
    def check(self):
        from numpy import cov,dot
        from numpy.linalg import inv
        if self.cov_matrix is not None and self.eigen_values is not None and self.eigen_vector_matrix is not None:
            for i in range(len(self.eigen_values)):
                # 'dot(cov_matrix,eigen_vector)-dot(eigen_values,eigen_vector)'
                print('checking eigen_value:',self.eigen_values.values[i])
                resu=dot(self.cov_matrix,self.eigen_vector_matrix.values[:,i])-dot(self.eigen_values.values[i],self.eigen_vector_matrix.values[:,i])
                print(resu.round(4))
                continue
            print('check covariance matrix:')
            from_pm=cov(self.principal_matrix,rowvar=False)
            print('re-calculate from self.principal_matrix:\n',from_pm.round(2))
            c=dot(inv(self.eigen_vector_matrix),self.cov_matrix)
            c=dot(c,self.eigen_vector_matrix)
            print('re-calculate from original cov_matrix:\n',c.round(2))
            print('check if match:\n',(from_pm-c).round(2))
            pass
        else:
            print('cov_matrix, eigen_values, and eigen_vector_matrix are None.')
        pass
    def simple_pca(self,in_matrix_df):
        '''
        Pass a pandas.DataFrame as argument and start a simple analysis, with the following procedures:
            1.clear current data;
            2.load the input DataFrame;
            3.data standardized;
            4.perform hierarchical clustering;
            5.perform principal component analysis.
        '''
        self.clear_data()
        self.load_matrix(in_matrix_df)
        self.standardize()
        self.hie_cluster()
        self.cov_pca()
        print(self.cur_data)
        print('checking...')
        self.check()
        self.clear_data()
        pass
    def start(self):
        '''
        Overwrite this method and customize your analysis.
        '''
        pass
if __name__=='__main__':
    pass

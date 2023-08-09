#!/usr/bin/env python
# coding=utf-8
from numpy.linalg import norm
from numpy import array,concatenate,dot,zeros
class GramSchmidt:
    '''
    To perform Gram-Schmidt Orthogonalization, initialize class GramSchmidt with input numpy.ndarray of matrix, and then GramSchmidt.start().
    '''
    def __init__(self,data_array):
        self.data_array=data_array
        self.data_set=[]
        for hv in self.data_array: # horizontal vector
            print('horizontal vector to perform Gram-Schmidt Orthogonalization:',hv)
            self.data_set.append(hv)
        self.projection_vector_set=[]
        self.out_set=[]
        self.smt_array=None
        pass
    def __vector_projection(self,u,v):
        '''
        projection of u on v.
        '''
        proj_index=dot(u,v)/dot(v,v)
        proj_vector=dot(proj_index,v)
        return proj_vector
    def start(self):
        self.out_set.append(self.data_set[0])
        self.projection_vector_set.append(self.__vector_projection(self.data_set[0],self.out_set[0]))
        n=1
        while n<len(self.data_set):
            target_vector=self.data_set[n]
            for i in range(0,len(self.out_set)):
                target_vector=target_vector-self.__vector_projection(target_vector,self.out_set[i])
                continue
            self.out_set.append(target_vector)
            # print('target_vector:',target_vector)
            n+=1
            pass
        # print('data length:',len(self.out_set),len(self.data_set))
        for i in range(len(self.out_set)):
            self.out_set[i]=self.out_set[i]/norm(self.out_set[i],2)
            continue
        final_smt=concatenate(self.out_set,axis=0).reshape(self.data_array.shape)
        self.smt_array=final_smt
        print('Result of row vectors:',self.smt_array)
        return  final_smt
    def check(self):
        if self.smt_array is not None:
            for i in self.smt_array:
                if norm(i,2)-1<0.00005:
                    print('2-norm = 1:',norm(i,2))
                continue
            for i in range(len(self.smt_array)):
                for j in range(len(self.smt_array)):
                    if i != j:
                        if dot(i,j) < 0.00005:
                            print('dot is zero:',dot(i,j))
                    continue
            pass
        else:
            print('self.smt_array is None, perform self.start() first.')
            pass
    pass
if __name__=='__main__':
    pass

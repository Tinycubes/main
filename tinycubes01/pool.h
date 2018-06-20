#ifndef __POOL_H
#define __POOL_H

#ifndef __TYPES_H
#dinclude __TYPES_H
#endif


/******************************************************************************************//*

    nnStructPool, nnStruct

    Elemento generico que serve para qualquer estrutura de dados alocada dinamicamente.
    Foi projetado para encadear qualquer estrutura em POOLs

*//******************************************************************************************/

typedef struct s_struct {
   struct s_struct *next;
} nnStruct, *nnPStruct;

typedef struct {
    int max_n;
    int n;
    nnPStruct first;
} nnStructPool, *nnPStructPool;


int   nn_add_to_pool(nnPStructPool pool, void *someStruct);
void *nn_get_from_pool(nnPStructPool pool);


#endif

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

/*--------------------
*
* Adiciona a estrutura ao pool de Structures
*
*/
int nn_add_to_pool(nnPStructPool pool, void *someStruct) {
    nnPStruct s = (nnPStruct) someStruct;
    if (pool->n == pool->max_n) return 0;
    s->next = pool->first;
    pool->first = s;
    pool->n ++;
    return pool->n;
}


/*--------------------
*
* Pega um elemento do Pool
*
*/
void *nn_get_from_pool(nnPStructPool pool) {
    nnPStruct s;

    if (pool->n == 0) return NULL;
    s = pool->first;
    pool->first = s->next;
    s->next = NULL;
    pool->n --;
    return s;
}


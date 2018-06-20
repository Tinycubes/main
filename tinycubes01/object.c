#include "object.h"

/*--------------------
*
*
*/
#ifdef __TS__
nnPClass tn_create_class_ex(int size, int n_values,int n_chains,nnTimeFromObject getTime,int timeBase) {
#else
nnPClass tn_create_class(int size, int n_values) {
#endif

    nnPClass pclass = (nnPClass) calloc(sizeof(nnClass),1);
    //@@@@ Verificar size e garantir ser multiplo de 4 ou 8

    //
    if (!n_values) fatal("Missing number of values");

    pclass->size = size;
    pclass->n_values = n_values;

#ifdef __TS__
    pclass->getTime = getTime;
    pclass->timeBase = timeBase;
#endif

    return pclass;
}

/*--------------------
*
*
*/
nnPClass tn_create_class(int size, int n_values,int n_chains) {
    return tn_create_class_ex(size,n_values,n_chains,NULL,0);
}


/*--------------------
*
*
*/
nnPObject tn_create_object(nnPClass pclass,void *data) {
    nnPObject po;
    int size;

    // @@@@ Poderia haver um pool de objetos para reaproveitar alocacao

    // aloca um objeto
    size = sizeof(nnObject) + pclass->size;
    size -= sizeof(uchar);  // descarta o byte usado no struct por conta da compatibilidade
    po = (nnPObject) calloc(1,size);
    tn_stats.n_objects ++;

    po->sig = SIG_OBJECT;

    // aloca o vetor de valores
    po->pclass = pclass;
    po->n_values = pclass->n_values;
    po->values = (int *) calloc(po->n_values+pclass->n_chains+1,sizeof(int));

    // contabiliza o novo objeto criado
    pclass->count ++;

    // copia os dados para a area de armazenamento
    memcpy(po->data, data, pclass->size);

    // Appenda o objeto na lista de objetos
    po->next = NULL;

    // coloca o novo objeto no final da lista de todos os objetos
    if (pclass->last) {
        pclass->last->next = po;
    } else {
        pclass->first = po;
    }
    pclass->last = po;

    return po;
}

/*--------------------
*
*
*/
nnPObject tn_release_object(nnPObject o) {
    SIG_TEST(OBJECT,o);
    // @@@@ faltam mecanismos de defesa
    tn_stats.n_objects --;
    free(o);
    return NULL;
}

#ifdef __QUERY__

/*--------------------
*
*
*/
nnPObject tn_assign_object(nnPObject po,void *data) {
    if (!po) return NULL;
    SIG_TEST(OBJECT,po);

    // copia os dados para a area de armazenamento
    memcpy(po->data, data, po->pclass->size);

    return po;
}

#endif


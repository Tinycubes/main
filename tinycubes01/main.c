/******************************************************************************************//*

    NanoCubes for Real-Time Exploration os Spatiotemporal Datasets

    Nilson L. Damasceno

*//******************************************************************************************/

/*---------------------------------------------------------------------------------------*/

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <malloc.h>
#include <time.h>

#include <setjmp.h>

#include "nn.h"

#define __NOVO__
#define __WITH_DUMP__0
#define __OLD_DUMP__0

#define __TS__
#define __QUERY__

#include <string.h>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#ifdef _WIN32
#define PATH_SEP '\\'
#else
#define PATH_SEP '/'
#endif

#define MAXPATH 1024
static char path[MAXPATH];
static char tmp[MAXPATH];

#define LOOP \
    __pragma(warning(push)) __pragma(warning(disable:4127)) \
    while (1)  __pragma(warning(pop))

/******************************************************************************************//*

    Facilidades de trace e debug

*//******************************************************************************************/

#define SIG_TEST(W,X)  if ((X)->sig != SIG_##W) fatal("\nSIG ERROR: line=%d SIG:[%x] @ [%p]",__LINE__,(X)->sig,X);


#define trace1(A)
//tracef0(A)
#define trace2(A,B) tracef0(A,B)

#define trace3(A,B,C)
//tracef0(A,B,C)

#include <stdarg.h>

#define TRACEBUF_SZ 518
static char trace_buf[TRACEBUF_SZ];

void tracef0(char *fmt,...) {
    va_list args;

    fmt;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

int allow_print;

void qtrace(char *fmt,...) {
    va_list args;

    if (allow_print != 2) return;

    fmt;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}


// para remocao
void xprintf(char *fmt,...) {
#if 1
    fmt;
#else
    va_list args;
    fmt;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
#endif
}


// para remocao
void Rprintf(char *fmt,...) {
#if 0
    fmt;
#else
    va_list args;
    fmt;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    fflush(stdout);
#endif
}



// para adicao
void Iprintf(char *fmt,...) {
#if 0
    fmt;
#else
    va_list args;
    fmt;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
#endif
}

void error(char *fmt,...) {
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr,fmt, args);
    va_end(args);
}

static int recover = 0;
static jmp_buf jmp;

static jmp_buf *fatal_jmp = NULL;

static void  set_fatal_mode(jmp_buf *jmp ) {
    fatal_jmp = jmp;
}

void fatal(char *fmt,...) {
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr,fmt, args);
    va_end(args);
    fflush(stdout);

    if (fatal_jmp) longjmp(*fatal_jmp,10);
    exit(100);
}


static struct {
    int n_ref_objects;
    int n_objects;
    int n_iguais;
    int n_terminal;
    int n_nodes;
    int n_content;
    int n_alphas;
    int n_betas;
    int n_edges;
} nn_stats;

#define __STATS__

#ifdef __STATS__
/*--------------------
*
*
*
*/
void show_stats(void) {
    return;
    printf("n_alphas %d\n",nn_stats.n_alphas);
    printf("n_betas %d\n",nn_stats.n_betas);
    printf("n_terminal %d\n",nn_stats.n_terminal);
    printf("n_nodes %d\n",nn_stats.n_nodes);
    printf("n_objects %d\n",nn_stats.n_objects);
    printf("n_edges %d\n",nn_stats.n_edges);
    printf("n_iguais %d\n",nn_stats.n_iguais);
//    printf("n_content %d\n",nn_stats.n_content);
//    printf("n_ref_objects %d\n",nn_stats.n_ref_objects);
}
#endif

/******************************************************************************************//*

    Tipos utilizados que podem exigir adaptações dependendo do compilador

*//******************************************************************************************/
typedef __int64          int64;
typedef unsigned __int64 uint64;
typedef unsigned int     uint;
typedef unsigned char    uchar;
typedef signed char      schar;

#pragma pack(8)

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


/******************************************************************************************//*

    nnObject, nnClass

    Assume que todos os objetos a serem indexados são estruturalmente iguais
    Note que os objetos poderiam ter estruturas completamente diferentes
    sendo similares apenas pelas funções, como duck typing.

*//******************************************************************************************/

#define SIG_OBJECT  0x48282134

typedef struct nn_object {
  int sig;
  struct nn_class  *pclass;          // Classe do Objeto, ver abaixo
  struct nn_object *next;            // encadeamento na lista de todos os objetos
                                     // Nao é necessário para o algoritmo original
                                     // Mas é util para varrer todos os objetos
  // $NOVO
  int              ok;
  int              n_values;
  int              *values;         // valores resultantes do mapeamento d

  uchar             data[1];         // area de armazenamento dos dados
                                     // poderia ser data[0], mas pode não ser portável entre compiladores
}  nnObject, *nnPObject;

#ifdef __TS__
typedef int (*nnTimeFromObject) (nnPObject);
#endif

typedef struct nn_class {
    int size;                    // Tamanho dos objetos da classe
    int n_values;
    int n_chains;

#ifdef __TS__
    nnTimeFromObject getTime;
    int timeBase;
#endif

    int count;                   // Numero de objetos instanciados (util)
    nnPObject first, last;       // lista encadeada para objetos da classe
                                 // Nao e necessario mas eh util
} nnClass, *nnPClass;

/*--------------------
*
*
*/
#ifdef __TS__
nnPClass nn_create_class_ex(int size, int n_values,int n_chains,nnTimeFromObject getTime,int timeBase) {
#else
nnPClass nn_create_class(int size, int n_values) {
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
nnPClass nn_create_class(int size, int n_values,int n_chains) {
    return nn_create_class_ex(size,n_values,n_chains,NULL,0);
}


/*--------------------
*
*
*/
nnPObject nn_create_object(nnPClass pclass,void *data) {
    nnPObject po;
    int size;

    // @@@@ Poderia haver um pool de objetos para reaproveitar alocacao

    // aloca um objeto
    size = sizeof(nnObject) + pclass->size;
    size -= sizeof(uchar);  // descarta o byte usado no struct por conta da compatibilidade
    po = (nnPObject) calloc(1,size);
    nn_stats.n_objects ++;

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
nnPObject nn_release_object(nnPObject o) {
    SIG_TEST(OBJECT,o);
    // @@@@ faltam mecanismos de defesa
    nn_stats.n_objects --;
    free(o);
    return NULL;
}

#ifdef __QUERY__

/*--------------------
*
*
*/
nnPObject nn_assign_object(nnPObject po,void *data) {
    if (!po) return NULL;
    SIG_TEST(OBJECT,po);

    // copia os dados para a area de armazenamento
    memcpy(po->data, data, po->pclass->size);

    return po;
}

#endif


/******************************************************************************************//*

    nnValue

    Union que armazena "todos" os tipos de valores possíveis.
    Não tem o tag identificador do tipo porque as operações de LabelFunction
    correspondentes ao dominio/chain "entendem" o que está armazenado.

*//******************************************************************************************/
typedef union {
//    int64    i64;
//    uint64   ui64;
    int      i;
    uint     ui;
    void     *ptr;
    double  val;
} nnValue, *nnPValue;

/*---------------------------------------------------------------------------------------*//*
   Tipos dos Ponteiros para Funções que manipulam nnValue
*//*---------------------------------------------------------------------------------------*/

// O parametro pos indica a posicao/indice na chain.
// Ele e utilizado para multiplexar todas as LabelFunctions da Chain em uma unica
// LabelFunction
typedef     void    (*nnGetValue)     (nnPObject po,  int pos, nnPValue pv);

// Retorna 0 se os 2 valores são diferentes e 1 se são iguais.
typedef     int     (*nnAreEqualValues)(nnPValue pv1, nnPValue pv2);

// Converte um objeto para string (apenas para trace)
typedef      char *  (*nnObjectToString)      (nnPObject po, int pos, char *buf, int maxlen);

// Converte um valor para string (apenas para trace)
typedef      char *  (*nnValueToString)      (nnPValue v, int pos, char *buf, int maxlen);


/******************************************************************************************//*

    nnLabelFunction

*//******************************************************************************************/

typedef struct {
  nnGetValue         get_value;
  nnAreEqualValues   are_equal;

  nnObjectToString   object_to_string;
  nnValueToString    value_to_string;
} nnLabelFunction, *nnPLabelFunction;

static void default_get_value(nnPObject o, int pos, nnPValue v) {
    o; pos;
    v->i = 0;
}

static int default_equal_values(nnPValue pv1, nnPValue pv2) {
    return pv1->i == pv2->i;
}

static char * default_object_to_string(nnPObject o, int pos, char *buf, int maxlen) {
    o; pos; maxlen;
    *buf = '\0';
    return buf;
}

static char * default_value_to_string(nnPValue v, int pos, char *buf, int maxlen) {
    v; pos; maxlen;
    *buf = '\0';
    return buf;
}

/*--------------------
*
*
*/
nnPLabelFunction nn_create_label_function(  nnGetValue get_value,
                                            nnAreEqualValues are_equal,
                                            nnObjectToString object_to_string,
                                            nnValueToString value_to_string) {
    nnPLabelFunction lf = (nnPLabelFunction) calloc(sizeof(nnLabelFunction),1);

    lf->get_value = get_value ? get_value : default_get_value;
    lf->are_equal = are_equal ? are_equal : default_equal_values;
    lf->object_to_string = object_to_string? object_to_string : default_object_to_string;
    lf->value_to_string = value_to_string? value_to_string : default_value_to_string;

    return lf;
}

/*--------------------
*
*
*/
nnPLabelFunction nn_release_label_function(nnPLabelFunction lf) {
    free(lf);
    return NULL;
}

/******************************************************************************************//*

    nnChain

*//******************************************************************************************/

typedef struct nn_chain {
    int k;                    // numero de elementos na chain
    nnPLabelFunction lf;      // label function generica
    int offset;               // deslocamento no vetor de valores
} nnChain, *nnPChain;

/*--------------------
*
*
*/
nnPChain nn_create_chain(int number_of_levels, nnPLabelFunction lf) {
    nnPChain chain = (nnPChain) calloc(sizeof(nnChain),1);
    chain->k = number_of_levels;
    chain->lf = lf;
    return chain;
}

/*--------------------
*
*
*/
nnPChain nn_release_chain(nnPChain chain) {
    // @@@@ talvez seja o caso de dar free na lf. Mas tem de ser melhor analisado porque.
    // diferentemente da linguagem Rusty, nao existem controles sobre ponteiros que
    // compartilham apontamentos

    // nn_release_label_function(chain->lf);
    free(chain);
    return NULL;
}


/******************************************************************************************//*

    ITerminal

*//******************************************************************************************/

#define SIG_TERMINAL  0x44219333
#define SIG_TERMINAL_REMOVED 0x7A289818

typedef struct nn_terminal {
    int sig;
    int n;
} nnTerminal, *nnPTerminal;

typedef  nnPTerminal (*TerminalCreate)   (void);
typedef  nnPTerminal (*TerminalClone)    (nnPTerminal pt);
typedef  nnPTerminal (*TerminalRelease)  (nnPTerminal pt);
typedef  int         (*TerminalInsert)   (nnPTerminal pt,nnPObject obj);
typedef  void        (*TerminalRemove)   (nnPTerminal pt,nnPObject obj);
typedef  int         (*TerminalCount)    (nnPTerminal pt);
typedef  int         (*TerminalQuery)    (nnPTerminal pt, nnPObject obj, void* query);

typedef struct {
    /* interface mandatória */
    TerminalCreate      create;
    TerminalClone       clone;
    TerminalRelease     release;
    TerminalInsert      insert;
    TerminalRemove      remove;
    TerminalCount       count;

    /* interface para Nanocubes */
    TerminalQuery       query;
} ITerminal, *PITerminal;


nnPTerminal terminal_create(PITerminal pit) {
    //nn_stats.n_terminal ++;
    return pit->create();
}

nnPTerminal terminal_clone(PITerminal pit, nnPTerminal pt) {
    //nn_stats.n_terminal += pit->count(pt);
    return pit->clone(pt);
}

nnPTerminal terminal_release(PITerminal pit, nnPTerminal pt) {
    //nn_stats.n_terminal --;
    return pit->release(pt);
}

int terminal_insert (PITerminal pit, nnPTerminal pt, nnPObject obj) {
    return pit->insert(pt,obj);
}

void terminal_remove (PITerminal pit, nnPTerminal pt, nnPObject obj) {
    pit->remove(pt,obj);
}

int  terminal_count (PITerminal pit, nnPTerminal pt) {
    return pit->count(pt);
}


#ifdef __NOVO__


/******************************************************************************************//*

     nnSchema

*//******************************************************************************************/

typedef struct nn_schema {
    int       n_values;
    PITerminal pit;            // ponteiro para interface de terminal

    int       n_chains;        // assert(n_chains <= pnc->n)
    nnPChain  chains[1];       // contem todas as chains planejadas para o schema
                               // deveria ser chains[0], mas não eh por portabilidade
} nnSchema, *nnPSchema;

/*--------------------
*
*
*/
nnPSchema nn_create_schema(int number_of_chains) {
    nnPSchema s;
    int size;

    size = sizeof(nnSchema); //
    size += (number_of_chains * sizeof(nnChain)); // @@@@ esta alocando uma nnPChain a mais, mas ok
    s = (nnPSchema) calloc(size,1);

    s->n_values = 0;
    s->n_chains = number_of_chains;
    return s;
}

/*--------------------
*
*
*/
int nn_schema_set_chain(nnPSchema ps,int i,nnPChain chain) {
    int offset;

    if (i < 1) return 0;     // @@@@ erro severo.
    if (i > ps->n_chains) return 0; // @@@@ erro severo. Ultrapassou o numero de chains
    ps->chains[i-1] = chain;

    // $NOVO
    // ajusta o deslocamento
    offset = 0;
    ps->n_values = 0;
    for (i=0; i < ps->n_chains; i++) {
        xprintf("Chain %d Offset %d\n",i,offset);
        if (!ps->chains[i]) continue;
        ps->chains[i]->offset = offset;
        ps->n_values +=  ps->chains[i]->k;
        offset = offset + ps->chains[i]->k + 1;
    }

    return 1;
}

/*--------------------
*
*
*/
nnPSchema nn_release_schema(nnPSchema schema) {
#if 0
    // @@@@ deveria desalocar os chains alocados? provavelmente
    // Desaloca as chains alocadas
    int i;
    for (i = 0; i < schema->n; i++) {
        nn_release_chain(schema->chains[i]);
    }
#endif
    free(schema);
    return NULL;
}


/******************************************************************************************//*

    nnNode e nnBranch

    nnBranch armazena uma "aresta" que interliga um
    node pai e um node filho baseado em um valor.

    Cada node Pai tem uma lista de nnBranch, onde cada elemento tem
    um valor e um child, onde esse child pode ou não ser SHARED

*//******************************************************************************************/

#define HAS_CHILD(X) ((X)->branches)
#define HAS_NO_CHILD(X) ((X)->branches == NULL)
#define HAS_SINGLE_CHILD(X) (HAS_CHILD(X) && ((X)->branches->next == NULL))
#define HAS_MULTI_CHILD(X) (HAS_CHILD(X) && ((X)->branches->next != NULL))

#ifdef __NOVO__
#define SHARED_CONTENT        0
#define ALPHA_CONTENT         1
#define BETA_CONTENT          2
#define OMEGA_CONTENT         4
#define BETA_OMEGA_CONTENT    (BETA_CONTENT | OMEGA_CONTENT)

#define IS_ALPHA(NODE)           ((NODE) & ALPHA_CONTENT)
#define IS_BETA(NODE)            ((NODE) & BETA_CONTENT)
#define IS_OMEGA(NODE)           ((NODE) & OMEGA_CONTENT)

#endif

#define SIG_NODE         0x76234102
#define SIG_NODE_REMOVED 0x84391345

typedef struct nn_node {
    int sig;
    struct nn_branch *branches;

#ifdef __NOVO__
    int               counter;    // registra quantos objetos/paths esta "time_series" acumula
#else
    union {
        struct nn_branch *branches;   // lista de valores deste node
    } info;
#endif

#ifdef __NOVO__
    int content_type;    // $NOVO Vai substituir os itens abaixo
#endif
    int shared_content;  // Indica que o conteudo e compartilhado
    int terminal;        // Indica que o conteudo e uma Time Series

    //
    int dimension;       // apenas para o dump

    union {
        struct nn_node        *content;
        struct nn_terminal    *terminal;
        struct nn_node        *next;
    } c_t_n;

} nnNode, *nnPNode;

typedef struct {
    nnPNode first;
} nnNodesToFree, *nnPNodesToFree;

#define SIG_BRANCH  0x63829153

typedef struct nn_branch {
    struct nn_branch *next;

    nnValue         value;
    nnPNode         child;
    int             shared_child;
    int    sig;
} nnBranch, *nnPBranch;


/*--------------------------
*
*
*
*/
nnPNode nn_create_node(void) {
    nnPNode res =  (nnPNode) calloc(sizeof(nnNode),1);
    res->sig = SIG_NODE;
    nn_stats.n_nodes++;
    return res;
}

/*--------------------------
*
*
*
*/
nnPNode nn_release_node(nnPNode node) {
    SIG_TEST(NODE,node);
    node->branches = NULL;
    nn_stats.n_nodes--;
    free(node);
    return NULL;
}


/*--------------------------
*
*
*
*/
nnPNode nn_node_to_free(nnPNodesToFree list, nnPNode node, nnPNode redirect) {
    SIG_TEST(NODE,node);
    memset(node,0xCC,sizeof(nnNode));
    node->sig = SIG_NODE_REMOVED;
    node->c_t_n.next = list->first;

    node->branches = (nnPBranch) redirect;   // Salva o no previo para recupe

    list->first = node;
    return node;
}



/*--------------------------
*
*
*
*/
nnPBranch nn_create_value_child(void) {
    nnPBranch vc =  (nnPBranch) calloc(sizeof(nnBranch),1);
    vc->sig = SIG_BRANCH;
    nn_stats.n_edges ++;
    return vc;

}

/*--------------------------
*
*
*
*/
nnPBranch nn_release_value_child(nnPBranch p) {
    SIG_TEST(BRANCH,p);
    nn_stats.n_edges--;
    memset(p,0,sizeof(nnBranch));
    free(p);
    return NULL;
}


/******************************************************************************************//*


*//******************************************************************************************/


/*--------------------------
*
*
*
*/
void nn_release_nodes_to_free(nnPNodesToFree rlist) {
    nnPNode next, node;

    // esvazia a lista de nodes coletados
    for (node = rlist->first; node; node = next) {
        Rprintf("Releasing [%p]",node);
        next = node->c_t_n.next;
        SIG_TEST(NODE_REMOVED,node);
        node->sig = SIG_NODE;
        nn_release_node(node);
    }
    Rprintf("\n");
}




/******************************************************************************************//*

    nnNodes
    Implementa a lista updated_nodes e os "metodos" necessarios a ela
    INSERT in Nodes e NODE IN NODES

*//******************************************************************************************/

typedef struct nn_node_item {
    struct nn_node_item *next;
    nnPNode node;
} nnNodeItem, *nnPNodeItem;

typedef struct {
    nnPNodeItem first;
} nnNodes, *nnPNodes;

typedef nnPNodes nnPDeleted;

/*--------------------------
*
*
*
*/
nnPNodes nn_create_nodes(void) {
    return (nnPNodes) calloc(sizeof(nnNodes),1);
}

/*--------------------------
*
* Poderia se beneficiar de um pool de nnNodeItem
*
*/
void nn_release_nodes(nnPNodes nodes,int deep) {
    nnPNodeItem p, pn;
    for (p=nodes->first; p; p=pn) {
        pn = p->next;
        if (deep) nn_release_node(p->node);
        free(p);
    }
    free(nodes);
}

/*--------------------------
*
*
*
*/
int nn_node_in_nodes(nnPNodes nodes,nnPNode node) {
    nnPNodeItem item;
    for (item = nodes->first; item; item=item->next) {
        if (item->node == node) return 1;
    }
    return 0;
}

/*--------------------------
*
*
*
*/
void  nn_insert_in_nodes(nnPNodes nodes,nnPNode node) {
    nnPNodeItem item;

    item = (nnPNodeItem) calloc(sizeof(nnNodeItem),1);
    item->node = node;
    item->next = nodes->first;
    nodes->first = item;
}

// $NOVO
/*--------------------------
*
*
*
*/
void  nn_append_to_nodes(nnPNodes nodes,nnPNode node) {
    nnPNodeItem item;

    for (item = nodes->first; item; item = item->next) {
        if (!item->next) break; // para no ultimo item da lista
    }

    if (!item) {
        item = (nnPNodeItem) calloc(sizeof(nnNodeItem),1);
        nodes->first = item;
    } else {
        item->next = (nnPNodeItem) calloc(sizeof(nnNodeItem),1);
        item = item->next;
    }
    item->node = node;
    item->next = NULL;
}

/******************************************************************************************//*

    Tree

*//******************************************************************************************/

#define OP_NONE          0
#define OP_FORK          1    // sinaliza que foi criado um novo ramo
#define OP_SHARE         2    // sinaliza que foi criado um shared child
#define OP_DESHARE       3    // sinaliza que um shared child foi separado
#define OP_RESHARE       4    // sinaliza que um share foi reapontado
#define OP_SKIP          5    // sinaliza que foi criado um shared child
#define OP_DOUBLE        6    // sinaliza que um terminal agora tem 2 valores

#define MAX_PATH  200
typedef struct s_xtree {
    int     done;
    int     op;                // indica a primeira operacao ocorreu na arvore
    int     n;
    nnPNode path[MAX_PATH];    // armazena o caminho para esta aresta
    struct  s_xtree *src;      // Xtree
    nnPNode root;
} nnXtree, *nnPXtree;


/*--------------------------
*
*
*
*/
void nn_xtree_inherits_rest(nnPXtree xtree,int level) {
    int i;
    if (!xtree->src) return;
    for (i = 0; i<xtree->n; i++) {
        xtree->path[i] = xtree->src->path[i];
    }
}


/*--------------------------
*
*
*
*/
nnPXtree nn_xtree_prepare(nnPXtree xtree,nnPXtree src,int level,nnPNode root) {
    level = level;

    xtree->src = src;
    xtree->done = 0;
    xtree->op = OP_NONE;
    xtree->root = root;
    // pode ser otimizado
    if (!src) memset(xtree->path,0,MAX_PATH * sizeof(nnPNode));

    return xtree;
}



// $NOVO
typedef struct  s_head {
        nnPNode node;         // no o node que sofreu a operacao
        int     dimension;    // dimensão (chain) onde a operacao ocorreu.
        int     height;       // altura onde a operacao ocorreu.
        nnPBranch vc;         // Branch para o head.node
        int value_index;
} nnHead, *nnPHead;

typedef struct s_tree {
    int     op;                // indica a primeira operacao ocorreu na arvore
    int     done;

    nnHead head;

    int     incr;              // incremento
    int     base_dim;          // dimensao base da arvore
    struct  s_tree *src;       // arvore anterior
//    int     changed;           // flag de modificacao
//    int     forked;         //
} nnTree, *nnPTree;



/*--------------------------
*
*
*
*/
nnPTree nn_init_tree(nnPTree tree,int base_dimension,nnPTree last_tree) {
    memset(tree,0,sizeof(nnTree));
    if (last_tree) {
        memcpy(&tree->head,&last_tree->head,sizeof(nnHead));
        tree->src = last_tree;
    }
    // tree->op = OP_NONE;
    tree->base_dim = base_dimension;

    return tree;
}


/*--------------------------
*
*
*
*/
nnPTree nn_create_tree(void) {
    nnPTree tree = (nnPTree) calloc(sizeof(nnPTree),1);
    return tree;
}


/*--------------------------
*
*
*
*/
nnPTree nn_release_tree(nnPTree tree) {
    free(tree);
    return NULL;
}


/******************************************************************************************//*

    Stack para Nodes

*//******************************************************************************************/

typedef struct nn_stack_item {
    nnPNode pn;
    int     dimension;   // $NOVO
    void    *ptr;        // ptr
    struct nn_stack_item *next;
} nnStackItem, *nnPStackItem;

typedef struct {
    nnPStackItem top;
} nnStack, *nnPStack;

/*--------------------------
*
*
*
*/
nnPStack nn_create_stack(void) {
    return (nnPStack) calloc(sizeof(nnStack),1);
}


/*--------------------------
*
*
*
*/
void nn_push(nnPStack ps, nnPNode node) {
    nnPStackItem item = (nnPStackItem) calloc(sizeof(nnStackItem),1);
    item->next = ps->top;
    item->pn = node;
    ps->top = item;
}

/*--------------------------
*
*
*
*/
nnPNode nn_pop(nnPStack stack) {
    nnPStackItem item = stack->top;
    nnPNode pn;
    if (!item) return NULL;  // @@@@ Excecao!!!
    stack->top = item->next;
    pn = item->pn;
    free(item);
    return pn;
}

// $NOVO
/*--------------------------
*
*
*
*/
void nn_push_ex(nnPStack ps, nnPNode node,int dimension,void *ptr) {
    nnPStackItem item = (nnPStackItem) calloc(sizeof(nnStackItem),1);
    item->next = ps->top;
    item->pn = node;
    item->ptr = ptr;
    item->dimension = dimension;
    ps->top = item;
}

// $NOVO
/*--------------------------
*
*
*
*/
nnPNode nn_pop_ex(nnPStack stack,int *dimension,void **ptr) {
    nnPStackItem item = stack->top;
    nnPNode pn;
    if (!item) return NULL;  // @@@@ Excecao!!!
    stack->top = item->next;
    *dimension = item->dimension;
    *ptr = item->ptr;
    pn = item->pn;
    free(item);
    return pn;
}



/*--------------------------
*
*
*
*/
int nn_stack_is_empty(nnPStack stack) {
    return ! stack->top;
}

/*--------------------------
*
*
*
*/
nnPStack nn_clear_stack(nnPStack ps) {
    // $NOVO
    while (!nn_stack_is_empty(ps)) nn_pop(ps);
    return ps;
}

/*--------------------------
*
*
*
*/
nnPStack nn_release_stack(nnPStack ps) {
    // $NOVO
    nn_clear_stack(ps);

    free(ps);
    return NULL;
}


/******************************************************************************************//*

    Funcoes auxiliares para montar o NanoCube

*//******************************************************************************************/

// funcoes auxiliares para smart_dump
int print_stats_flag = 0;
#ifdef __MAP__
int register_root(int dim,nnPNode node);
char *map_root_ex(int dim,nnPNode node);
char *map_value(nnPBranch vc);
#endif

#define ASTERISK_VALUE  (-9)

/*--------------------------
*
*
*
*/
void print_values(nnPObject po,int max) {
    int  i;
    if (max == -1) {
        max = 28;
    }
    for (i=0; i< max; i++) {
        printf("%02d ",po->values[i]);
    }
    printf("\n");
}


/*--------------------------
*
*
*
*/
void nn_prepare_object_values(nnPObject o,nnPSchema S) {
    int d, i;
    nnPChain pc;
    nnValue v;

    if (o->sig != SIG_OBJECT) fatal("SIG_OBJECTL at prepare");

    for (d=0;d<S->n_chains;d++) {
        pc = S->chains[d];
        for (i=0; i<pc->k; i++) {
            pc->lf->get_value(o,i+1,&v);
            o->values[pc->offset + i] = v.i;
        }
        o->values[pc->offset + i] = ASTERISK_VALUE;
    }

    o->ok = 1;
}



/*--------------------------
*
*
*
*/
int nn_has_single_child(nnPNode node) {
    if (node->branches == NULL) return 0;
    return node->branches->next == NULL;
}

/*--------------------------
*
*
*
*/
nnPNode nn_find_child_by_ivalue(nnPNode node, int ival, nnPBranch *vc_out) {
    nnPBranch vc;
    if (!vc_out) fatal("VC_OUT 01");
    SIG_TEST(NODE,node);
    // varre todos os valores do node
    for (vc = node->branches; vc; vc = vc->next) {
        if (ival == vc->value.i)  {
            if (vc_out) *vc_out = vc;
            return vc->child;
        }
    }
    return NULL;
}

#define __ORDERED

/*--------------------------
*
*
*
*/
nnPBranch nn_add_branch(nnPBranch *nodes, nnPNode child, nnPValue v,int shared_flag) {
    nnPBranch vc = nn_create_value_child();

    // inicializa child
    memcpy(&vc->value,v,sizeof(nnValue));
    vc->child = child;
    vc->shared_child = shared_flag;

#ifdef __ORDERED__
    {
        nnPBranch p,prev;

        vc->next = NULL;
        for (p = *nodes, prev = NULL; p; prev = p, p = p->next);
        if (!prev) {
            *nodes = vc;
        } else {
            prev->next = vc;
        }
    }
#else
    // insere o novo child na lista
    vc->next = *nodes;
    *nodes = vc;
#endif
    return vc;
}

#define PROPER_CHILD 0
#define SHARED_CHILD 1

/*--------------------------
*
*
*
*/
nnPNode nn_new_proper_child(nnPNode node, nnPValue v) {
    nnPNode child = nn_create_node();
    if (node->sig != SIG_NODE) fatal ("SIG_NODE 663");
    nn_add_branch(&node->branches,child,v,PROPER_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_proper_child_ex(nnPNode node, nnPValue v, nnPBranch *vc) {
    nnPNode child = nn_create_node();
    if (node->sig != SIG_NODE) fatal ("SIG_NODE 633");
    *vc = nn_add_branch(&node->branches,child,v,PROPER_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_proper_child_ival(nnPNode node, int ival) {
    nnValue v;
    nnPNode child = nn_create_node();

    v.i = ival;
    nn_add_branch(&node->branches,child,&v,PROPER_CHILD);
    return child;
}


/*--------------------------
*
*
*
*/
nnPNode nn_new_shared_child(nnPNode node, nnPValue v,nnPNode child) {
    if (node->sig != SIG_NODE) fatal ("SIG_NODE 733");
    nn_add_branch(&node->branches,child,v,SHARED_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_shared_child_ival(nnPNode node, int ival,nnPNode child) {
    nnValue v;
    v.i = ival;
    nn_add_branch(&node->branches,child,&v,SHARED_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_shared_child_ex(nnPNode node, nnPValue v,nnPNode child, nnPBranch *vc) {
    *vc = nn_add_branch(&node->branches,child,v,SHARED_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
void nn_set_shared_content(nnPNode receiver, nnPNode source) {
    // $NOVO
#ifdef __NOVO__
    receiver->content_type = SHARED_CONTENT;
#endif
    receiver->shared_content = 1;
    receiver->terminal = source->terminal;
    if (source->terminal) {
        SIG_TEST(TERMINAL,source->c_t_n.terminal);
        receiver->c_t_n.terminal = source->c_t_n.terminal;
    } else {
        SIG_TEST(NODE,source->c_t_n.content);
        receiver->c_t_n.content = source->c_t_n.content;
    }
}

#ifndef __NOVO__
/*--------------------------
*
*
*
*/
void nn_set_proper_content(nnPNode receiver, nnPNode node) {
    receiver->shared_content = 0;
    receiver->terminal = 0;
    receiver->c_t_n.content = node;
}
#endif

/******************************************************************************************//*

    Time Series Functions para o algoritmo

*//******************************************************************************************/

#ifdef __NOVO__
/*--------------------------
*
*
*
*/
void nn_set_proper_terminal_ex(nnPNode receiver, nnPTerminal pt,int content) {
    receiver->shared_content = 0;
    if (!IS_OMEGA(content)) fatal("Not Omega");
    receiver->content_type = content;
    receiver->terminal = 1;
    SIG_TEST(TERMINAL,pt);
    receiver->c_t_n.terminal = pt;
}
#else
/*--------------------------
*
*
*
*/
void nn_set_proper_time_series(nnPNode receiver, nnPTerminal pt) {
    receiver->shared_content = 0;
    receiver->terminal = 1;
    receiver->c_t_n.terminal = pt;
}
#endif


/******************************************************************************************//*

    Function Shallow Copy

*//******************************************************************************************/

/*--------------------------
*
*
*
*/
nnPNode nn_shallow_copy(nnPNode node) {
    nnPNode new_node;
    nnPBranch vc;

    new_node = nn_create_node();

    // set shared content
    nn_set_shared_content(new_node,node);

    // passa por todos os value-childs do node
    for (vc = node->branches; vc; vc = vc->next) {
        // adiciona em new_node novos registros do tipo value_child
        // onde o campo child é shared (vc->child)
        nn_new_shared_child(new_node, &vc->value, vc->child);
    }
    return new_node;
}



#ifdef __NOVO__

/******************************************************************************************//*

    $NOVO $NOVO $NOVO
    Alteracoes para poder fazer a contagem dos ramos

*//******************************************************************************************/

/*--------------------------
*
*
*
*/
nnPNode nn_set_proper_content_ex(nnPNode receiver, nnPNode node,int mode) {
    receiver->shared_content = 0;
    receiver->terminal = 0;
    receiver->content_type = mode;
    receiver->c_t_n.content = node;
    return receiver;
}

/*--------------------------
*
*  @@@@ Como esta função não foi descrita no paper, tive de advinhar o que fazia
*
*/
nnPNode nn_replace_child_ex(nnPBranch vc) {
    nnPNode child;

    //if (HAS_NO_CHILD(vc->child)) {
    //    aprintf("REPLACE CHILD - shared child HAS NO child. %p\n",vc->child->c_t_n.content);
    //    node = nn_create_node();
    //    if (vc->child->terminal) {
    //    } else {
    //        nn_set_shared_content(node,vc->child);
    //    }

    //    vc->child = node;
    //} else {
    //    aprintf("REPLACE CHILD - shared child HAS children.\n");
    //    vc->child = nn_shallow_copy(vc->child);;
    //}


    child = nn_shallow_copy(vc->child);
    vc->child = child;
    vc->shared_child = 0;

    return vc->child;
}


/*--------------------------
*
*
*
*/
void check_terminal_node(nnPNode node,nnPSchema S, int d,int level) {
    nnPBranch e;
    return;

    if (!node->branches) {
        // desce um nivel
        if (d != S->n_chains) {
            check_terminal_node(node->c_t_n.content,S,d+1,level+1);
            if (node->terminal) fatal("Should not be Terminal");
        } else {
            if (!node->terminal) fatal("Should be Terminal");
            SIG_TEST(TERMINAL,node->c_t_n.terminal);
            if (node->shared_content && (!node->branches)) fatal("Shared Content at lower level %d",level);
            // printf("bottom %d %d\n",d,level);
            return;
        }
    } else {
        if (d == S->n_chains) {
            if (node->shared_content && (level==S->n_chains+S->n_values)) fatal("Shared Content at lower level %d!",level);
        }

        for (e = node->branches; e; e=e->next) {
            check_terminal_node(e->child,S,d,level+1);
        }
    }
}

/*--------------------------
*
*
*
*/
int check_sums(nnPNode node,nnPSchema S, int d,int level,nnPObject obj) {
    nnPBranch e;
    //return 0;
    SIG_TEST(NODE,node);

    if (!node) return 0;

    if (!node->branches) {
        // desce um nivel
        if (d != S->n_chains) {
            if (node->terminal) fatal("SUM: Should not be Terminal dim:%d level:%d",d,level);
            if (node->terminal)
                return S->pit->count(node->c_t_n.terminal);
            else {
                if (!node->c_t_n.content) return 0;
                return (check_sums(node->c_t_n.content,S,d+1,d+S->chains[d]->offset,obj));
            }
        } else {
            if (!node->terminal)
                fatal("SUM: Should be Terminal");
            SIG_TEST(TERMINAL,node->c_t_n.terminal);
            if (node->shared_content && (!node->branches)) fatal("SUM: Shared Content at [%p] dim %d level %d!!",node,d,level);
            // printf("bottom %d %d\n",d,level);
            return S->pit->count(node->c_t_n.terminal);
        }
    } else {
        int sumc, sumbeta;
        if (d == S->n_chains) {
            if (node->shared_content && (level==S->n_chains+S->n_values))
                fatal("SUM: Shared Content at level %d!!!",level);
        }

        sumc = 0;
        for (e = node->branches; e; e=e->next) {
            if (e->child->sig == SIG_NODE)
                sumc += check_sums(e->child,S,d,level+1,obj);
        }
        if (node->terminal)
            sumbeta = S->pit->count(node->c_t_n.terminal);
        else
            sumbeta = check_sums(node->c_t_n.content,S,d+1,d+S->chains[d]->offset,obj);

        if (sumc != sumbeta) {
            printf("Somas [%p]:\n",node);
            for (e = node->branches; e; e=e->next) {
                nnPNode child = e->child;
                if (e->child->sig == SIG_NODE) {

                    if (!e->shared_child) {
                        printf("child[%p] Shared(N) (%d) sum:%d\n",child,e->value.i,check_sums(e->child,S,d,level+1,obj));
                    } else {
                        printf("child[%p] Shared(S) (%d)  sum:%d\n",child,e->value.i,check_sums(e->child,S,d,level+1,obj));
                        if (child->terminal) {
                            printf("    Terminal [%p] count: %d\n",child->c_t_n.terminal,terminal_count(S->pit,child->c_t_n.terminal));
                        }
                    }
                }
            }
            printf("Content: [%p] (%d) sum:%d\n",node->c_t_n.content,obj ? obj->values[level-1] : -1, sumbeta);
            fatal("Somas inconsistentes: content=%d children=%d node[%p] level:%d",sumbeta, sumc,node,level);
        }
        return sumc;
    }
}

/*--------------------------
*
*
*
*/
void check_ref_purge(nnPNode node,nnPSchema S, int d,int level,nnPNodes betas) {
    nnPBranch e;
    nnPNode content ;

    if (!node->branches) {
        // desce um nivel
        if (d != S->n_chains) {
            if (node->terminal) fatal("Should not be Terminal");
            content = node->c_t_n.content;
            if (nn_node_in_nodes(betas,content)) fatal("Invalid Reference");
            check_ref_purge(node->c_t_n.content,S,d+1,level+1,betas);
        } else {
            if (node->shared_content) fatal("Shared Content at lower level");
            if (!node->terminal) fatal("Should be Terminal");
            SIG_TEST(TERMINAL,node->c_t_n.terminal);
            // printf("bottom %d %d\n",d,level);
            return;
        }
    } else {
        if (d == S->n_chains) {
            if (node->shared_content && (level==S->n_chains+S->n_values)) fatal("Shared Content at lower level %d",level);
        } else {
            content = node->c_t_n.content;
            if (nn_node_in_nodes(betas,content)) fatal("Invalid Reference");
        }

        for (e = node->branches; e; e=e->next) {
            check_ref_purge(e->child,S,d,level+1,betas);
        }
    }
}

/*--------------------------
*
*
*
*/
void check_object_path(nnPNode root, nnPChain chain, nnPObject obj, int dimension, int principal) {
    int i;
    nnPNode child;
    nnPBranch vc;
    nnPNode node;
    int level;
    int ival;


    if (!root) fatal("root");
    node = root;

    level = chain->offset + dimension - 1; // reserva espaco para o child

    for (i = 0; i < chain->k; i++) {
        if (node->shared_content) {
            if (!root->branches) fatal("alpha node with shared content");
            if (root->branches->next) fatal("node with 2 or more childs and shared content");
        } else {

        }

        ival = obj->values[chain->offset+i];
        child = nn_find_child_by_ivalue(node,ival,&vc);

        // a: Nao encontrou uma aresta com o valor desejado
        if (!child) fatal("object not found");

        if (vc->shared_child) {
            break;
        }

        child = node;
    }
}

/******************************************************************************************//*

    Function TrailProperPath_ex

*//******************************************************************************************/



/*--------------------------
*
*
*
*/
nnPStack nn_trail_proper_path_ex(nnPNode root, nnPChain chain, nnPObject obj, int dimension, nnPXtree xtree) {
    int i;
    nnPStack stack;
    nnPNode child;
    nnPBranch vc;
    nnPNode node;
    int level;
    int ival;

    stack = nn_create_stack();
    nn_push(stack,root);

    if (xtree->done) return stack;

    node = root;
    level = chain->offset + dimension - 1; // reserva espaco para o child

    for (i = 0; i < chain->k; i++) {
        xtree->path[level] = node;

        ival = obj->values[chain->offset+i];
        child = nn_find_child_by_ivalue(node,ival,&vc);

        // a: Nao encontrou uma aresta com o valor desejado
        if (!child) {

            // a1: a Xtree esta num caminho Proper ou e a Xtree principal?
            if ((xtree->op == OP_FORK) || (xtree->src == NULL)) {

                // cria o novo node
                child = nn_new_proper_child_ival(node,ival);
                if (xtree->op == OP_NONE) { xtree->op = OP_FORK; }

            // a2: a Xtree origem tem modificacoes?
            } else if (xtree->src->op != OP_NONE) {
                child = xtree->src->path[level+1];                 // usa o child visitado
                if (!child) fatal("a2: child offset:%d level:%d",chain->offset,level+1);
                nn_new_shared_child_ival(node, ival, child);       // cria um Shared
                xtree->path[level+1] = child;                      // usa o child visitado

                xtree->done = 1;                                   // nao analisa mais nenhuma Ctree
                nn_xtree_inherits_rest(xtree,level);               // herda o restante do cominho
                if (xtree->op == OP_NONE) { xtree->op = OP_SHARE; }
                Iprintf("root[%p] Share child [%p] -> [%p]. OP=%d\n", xtree->root, node, child,xtree->op);

                break;

            // a3: child==NULL e Xtree origem sem modificacao
            } else {
                fatal("Erro: child==NULL e Xtree origem sem modificacao");
            }

        // b: encontrou a aresta com valor v e ela e Shared?
        } else if (vc->shared_child) {

            // b0: Esta na Xtree principal
            if (!xtree->src) {
                fatal("Erro: Shared Child in Xtree principal");

            // b1: A Xtree origem não foi modificada?
            } else if (xtree->src->op == OP_NONE) {
                nn_xtree_inherits_rest(xtree,level);
                //xtree->done = 1;
                break;

            // b2: o child encontrado está no caminho percorrido?
            } else if (child == xtree->src->path[level+1]) {
                nn_xtree_inherits_rest(xtree,level);
                xtree->done = 1;
                break;

            // b3: o child encontrado não está no caminho
            } else {
                nnPNode prevChild = vc->child;
                child = nn_replace_child_ex(vc);

                // use este child para os novos links relativos
                xtree->src->path[level+1] = child;

                if (xtree->op == OP_NONE) { xtree->op = OP_DESHARE; }
                Iprintf("root[%p] Replace child node [%p] [%p] => [%p]. OP=%d  Shared=%d\n",xtree->root,node,prevChild,child,xtree->op,vc->shared_child);
            }

        // c: encontrou a aresta com valor ival e ela não era Shared
        } else {
            // continua o caminho
        }

        nn_push(stack,child);
        node = child;
        level = level + 1;
    }
    // coloca a ultima child no c
    xtree->path[level] = node;
    return stack;
}


/******************************************************************************************//*

    function nn_add_ex

*//******************************************************************************************/


/*--------------------------
*
*
*
*/
void nn_add_ex(nnPNode root, nnPObject o, int dimension, nnPSchema S, nnPNodes updated_nodes, nnPXtree xtree) {
    nnPStack stack;
    nnPNode node, child;
    int update;

    // $NOVO
    nnXtree new_xtree;
    nnPXtree param_tree;

    stack = nn_trail_proper_path_ex(root, S->chains[dimension-1], o, dimension, xtree);
    child = NULL;

    xprintf("[%d] root[%p] OP = %d\n",dimension,root,xtree->op);

    while (! nn_stack_is_empty(stack)) {

        node = nn_pop(stack);
        update = 0;

        //
        if (nn_has_single_child(node)) {
            // como a stack pode ter apenas a raiz,
            //    pode não haver child disponivel
            if (!child) { child = node->branches->child; }
            nn_set_shared_content(node,child);
            //printf("1 %p\n",node);
        //
        } else if (node->c_t_n.content == NULL) {
            xprintf("2\n");
            if (node->shared_content || node->terminal)
                fatal("**** Abnormal strucuture ****\n");

            if (dimension == S->n_chains) {
                update = HAS_NO_CHILD(node) ? OMEGA_CONTENT : BETA_OMEGA_CONTENT;
                nn_set_proper_terminal_ex(node,terminal_create(S->pit),update);
                if (IS_BETA(node->content_type)) fatal("Wrong Omega Beta");
                //printf("2a %p\n",node);

            } else {
                // indica que a arvore que sera atualizada é uma alpha
                update = ALPHA_CONTENT;
                nn_set_proper_content_ex(node, nn_create_node(),ALPHA_CONTENT);
                //printf("2b %p\n",node);
            }
            nn_stats.n_alphas++;

        // paper: if contentIsShared(node) and content(node) not in updated_nodes
        // o node esta marcado como shared mas tem mais de um filho!!
        } else if (node->shared_content) {
            xprintf("3a\n");

            // time series?
            if (dimension == S->n_chains) {
                //if (!nn_node_in_nodes(updated_nodes,(nnPNode) node->c_t_n.terminal)) {
                    nnPTerminal old,novo;
                    xprintf("3b\n");
                    if (!node->terminal) fatal("Should be terminal! %p -> %p",node,node->c_t_n.terminal);
                    SIG_TEST(TERMINAL,node->c_t_n.terminal);
                    novo = terminal_clone(S->pit, node->c_t_n.terminal);

                    old = node->c_t_n.terminal;
                    xprintf("[%d] root[%p] [%p] clonado para [%p] => %d\n",dimension,root,old,novo,terminal_count(S->pit,novo));
                    update = HAS_NO_CHILD(node) ? OMEGA_CONTENT : BETA_OMEGA_CONTENT;
                    nn_set_proper_terminal_ex(node, novo, update);
                    if (node->shared_content) fatal("Shared");
                    //printf("3a %p\n",node);

                    if (IS_BETA(update)) nn_stats.n_betas++; else nn_stats.n_alphas++;

                    // nao incrementa duas vezes
                    if (nn_node_in_nodes(updated_nodes,(nnPNode) old)) {
                        xprintf("found\n");
                        update = 0;
                    } else {
                        xprintf("Not found in nodes: %p\n",node->c_t_n.content);
                    }
                //}
            // usual content
            } else if (!nn_node_in_nodes(updated_nodes,node->c_t_n.content)) {

                nnPNode newnode,child;
                xprintf("3c\n");
                if (node->terminal) fatal("terminal");
                update = HAS_NO_CHILD(node) ? ALPHA_CONTENT : BETA_CONTENT;
                newnode = nn_shallow_copy(node->c_t_n.content);
                child = newnode->branches?newnode->branches->child:NULL;
                Iprintf("Shared to Proper Content [%p] => [%p] -> [%p] . Update: %d Copy proper content\n",node,newnode,child,update);
                nn_set_proper_content_ex(node, newnode, update);
                if (IS_BETA(update)) nn_stats.n_betas++;  else nn_stats.n_alphas++;
            } else {
                xprintf("3d\n");
            }
           // Original: update = 1;

        } else if (!node->shared_content && node->c_t_n.content) {
            xprintf("4\n[%d] root[%p]   node[%p]\n",dimension,root,node);
            trace1("Not Shared Content. Just update\n");

            // Original
            // update = 1;

            // $NOVO
            // indica que a arvore que sera atualizada seguira o content_mode do node
            update = node->content_type; // ALPHA_CONTENT ou BETA_CONTENT
            if (update == 0) fatal ("Erro!\n");
        }

        // $NOVO
        if (HAS_MULTI_CHILD(node) && (!IS_BETA(node->content_type) || node->shared_content)) {
            fatal("Inconsistencia 0");
        }

        if (update) {
            if (dimension == S->n_chains) {
                int c;
                if (!IS_OMEGA(update)) fatal("Not omega");
                //print_values(o,28);
                c = terminal_insert(S->pit,node->c_t_n.terminal,o);
                xprintf("Update[%d]: Inserting at [%p] => %d \n",dimension,node->c_t_n.terminal,c);
                if ((c >= 2) && (xtree->src == NULL)) nn_stats.n_iguais ++;
            } else {
                if (IS_ALPHA(update)) {
                    param_tree = xtree;

                } else if (IS_BETA(update)) {
                    param_tree = nn_xtree_prepare(&new_xtree, xtree, dimension+1,node->c_t_n.content);

                } else {
                    fatal("Abnormal execution: Not Alpha or Beta!!!");
                    param_tree = NULL;  // evitar warning
                }

                nn_add_ex(node->c_t_n.content, o, dimension+1, S, updated_nodes, param_tree);
            }

            // Ponto de INSERÇÃO no algoritmo do paper
            // segundo o paper teria aqui: nn_insert_in_nodes(updated_nodes,node->c_t_n.content);
            nn_insert_in_nodes(updated_nodes,node->c_t_n.content);
            xprintf("Inserted in nodes: %p\n",node->c_t_n.content);
            if (!nn_node_in_nodes(updated_nodes,node->c_t_n.content)) fatal("Missing node");
        }

        child = node;
    }

    nn_release_stack(stack);
    // trace2("end add: %d\n",dimension);
}

#endif


/*--------------------------
*
*
*
*/
void nn_do_purge(nnPNode node, nnPNode target, nnPNodesToFree rlist,PITerminal pit) {
    nnPBranch pb, next, dummy;
    nnPNode child;
    nnPNode target_child;

    Rprintf("Purging node[%p] target[%p]...\n",node,target);
    fflush(stdout);

    //printf("Purging %p\n",node);
    if (node->shared_content ^ (node->content_type == SHARED_CONTENT)) fatal("sdfjksdfklj");

    // purge todos os childs
    for (pb = node->branches; pb; pb = next) {
        //printf("1\n");    fflush(stdout);

        next = pb->next;
        child = pb->child;
        // SIG_TEST(NODE,child);

        if (!pb->shared_child && child->sig == SIG_NODE) {
            // localiza o node do target para onde os shareds de outras xtrees deve apontar
            target_child = nn_find_child_by_ivalue(target,pb->value.i,&dummy);
            if (!target_child) {
                // pode ter sido removido.
                Rprintf("Value %d NOT FOUND while purging node [%p]  target[%p] sig:%x\n",pb->value.i,node,target,target->sig);
                //fatal("Value %d not found. node [%p]  target[%p]",pb->value.i,node,target);
            } else {
                Rprintf("Value %d FOUND while purging node [%p]  target[%p]\n",pb->value.i,node,target);
            }

            // remove o child
            Rprintf("do_purge on proper child [%p]\n",child);
            if (child->sig == SIG_NODE_REMOVED) {
                Rprintf("Proper node was deleted!!!!\n");
            } else if (child->sig == SIG_NODE) {
                nn_do_purge(child, target_child, rlist, pit);
            }
        }
        Rprintf("Releasing edge...\n");
        nn_release_value_child(pb);
    }
    node->branches = NULL;
    //printf("2\n");    fflush(stdout);


    // Purge apenas Proper Content
    if (!node->shared_content) {
        //printf("3\n");    fflush(stdout);
        if (node->terminal) {
            nnPTerminal pt = node->c_t_n.terminal;
            if (pt->sig == SIG_TERMINAL) {
                SIG_TEST(TERMINAL,pt);
                Rprintf("$$$ do_purge: terminal_release [%p]\n",pt);
                terminal_release(pit,pt);
            } else fatal("No SIG_TERMINAL");
        } else {
            nnPNode content = node->c_t_n.content;
            if (content->sig == SIG_NODE) {
                Rprintf("$$$ do_purge: content_release [%p]\n",content);
                nn_do_purge(content, target->c_t_n.content, rlist, pit);
            }  else fatal("No SIG_NODE");
        }
        if (IS_BETA(node->content_type)) nn_stats.n_betas--; else nn_stats.n_alphas--;
        node->c_t_n.content = NULL;
    }

    //printf("8\n");    fflush(stdout);
    SIG_TEST(NODE,node);
    Rprintf("Purging node [%p]... ",node);
    nn_node_to_free(rlist,node,target);
    Rprintf("done\n");
}


/*--------------------------
*
*
*
*/
void nn_redirect_purged_nodes(nnPNode node) {
    nnPBranch pb;
    nnPNode child;

    if (node->sig == SIG_NODE_REMOVED) {
        Rprintf("//// Redirect return\n");
        return;
    }
    //fatal("removed");

    //printf("A1\n");    fflush(stdout);
    // relink todos os childs
    for (pb = node->branches; pb; pb = pb->next) {
        //printf("A2\n");    fflush(stdout);
        child = pb->child;
        //printf("A2 %p\n",child);    fflush(stdout);
        if (!child)
            fatal("Missing Child");

        if (!pb->shared_child) {
            //printf("A2a\n");    fflush(stdout);
            nn_redirect_purged_nodes(child);
        } else {
            //printf("A2b\n");    fflush(stdout);

            if (child->sig == SIG_NODE_REMOVED) {
                //printf("A2c\n");    fflush(stdout);
                if ( child->branches ) {
                    nnPNode new_child = (nnPNode) child->branches;
                    //printf("A2d\n");    fflush(stdout);
                    Rprintf("//// RELINK: node [%p]  value: %d [%p] => [%p]\n",node,pb->value.i,child,new_child);
                    pb->child = new_child;
                }
            }
            //printf("A2e\n");    fflush(stdout);
        }
    }

    //printf("A3\n");    fflush(stdout);
    // Relink apenas Proper Content
    if (!node->shared_content) {
        if (!node->terminal) {
            nn_redirect_purged_nodes(node->c_t_n.content);
        }
    }
       //printf("A4\n");    fflush(stdout);
}

/*--------------------------
*
*
*
*/
int nn_remove_edge_child(nnPNode node, nnPBranch edge) {
    nnPBranch vc, prev;

    // varre todos os filhos de node procurando child
    for (prev=NULL, vc=node->branches; vc; prev=vc, vc = vc->next) {

        if (vc != edge) continue;
        // xprintf("Removed Value Child %c\n",vc->value.i-10+65);

        // vc retira da lista
        if (prev) {
            prev->next = vc->next;
        } else {
            node->branches = vc->next;
        }
        vc->next = NULL;
        nn_release_value_child(vc);

        return 1;
    }

    fatal("ERRO SEVERO - 1229");
    return 0;
}


/*--------------------------
*
*  Retorna a raiz
*
*/
int nn_remove_step(nnPNode root,nnPSchema S, nnPObject obj,int dim,nnPNodesToFree rlist,nnPNodes betas) {
    nnPNode node = root;
    nnPNode child;
    nnPNode path[MAX_PATH];
    nnPBranch edge;
    nnPBranch edges[MAX_PATH];
    int level0 = S->chains[dim-1]->offset;
    int removeEdge = 0;
    int length = S->n_values + S->n_chains;
    int v;
    int level = level0;
    int purged = 0;
    int has_content;

    if (obj->sig != SIG_OBJECT) fatal("SIG_OBJECTL at release");

    Rprintf("******* Dimensao: %d lenght: %2d level: %2d  root:[%p]\n",dim,length,level,root);
    while (level < length) {
        path[level] = node;
        v = obj->values[level];

        //if (level >= length-5)
            Rprintf("Level: %2d  Node: [%p] Value:%2d\n",level,node,v);

        if (v == ASTERISK_VALUE) {
            //printf("level %d\n",level);
            if (node->shared_content) {
                if (node->branches) fatal("Shared Content and Children on path");
                if (!node->terminal) fatal("Terminal problem");
                //printf("level (shared) %d\n",level);
                break;
            }
            child = node->c_t_n.content;
            edges[level] = NULL;

        } else {
//            Rprintf("Finding Child: %d",v);
            child = nn_find_child_by_ivalue(node,v,&edge);
//            Rprintf(" Done\n");
            edges[level] = edge;
            if (!child) {
                //fatal("Value not found");
                Rprintf("********** Value not found!\n");
                return 0;
            } else if (edge->shared_child) {
                removeEdge = (child->sig == SIG_NODE_REMOVED && !child->branches);
                Rprintf(" BREAK ON Shared child [%p] -> [%p] %d. Remove Edge = %d\n",node,child,edge->value.i,removeEdge);
                break;
            }
        }
        node = child;
        level ++;
    }

    Rprintf("End Level: %d           Start Level:%d\n",level,level0);
    if (level >= length) {
        int c;
        nnPTerminal pt = (nnPTerminal) node;
        if (pt->sig != SIG_TERMINAL) {
            Rprintf("$$$ count: terminal_count [%p]\n",pt);
            removeEdge = 0;
        } else {
            c = terminal_count(S->pit,pt);
            Rprintf("Count before remove %d\n",c);
            terminal_remove(S->pit,pt,obj);
            Rprintf("Remove from terminal [%p]\n",pt);
            c = S->pit->count(pt);
            Rprintf("Count level0 %d after %d\n",level0,c);
            removeEdge = c == 0;
            if (removeEdge) {
                // nn_stats.n_betas--;
                Rprintf("$$$ end_level: terminal_release [%p]\n",pt);
                Rprintf("@@@@ Removing alpha terminal from xtree %s %p ...  ",level0==0?"principal":"derivada",pt);
                terminal_release(S->pit,pt);
                Rprintf("Done\n");
            }
        }
        level --;
    }

    Rprintf("delete loop until level %d\n",level0);
    //

    while (level >= level0) {
        node = path[level];
        edge = edges[level];
        v  = obj->values[level];
        has_content = 0;

        SIG_TEST(NODE,node);

        Rprintf("Remove? %d   Remove level: %d value:%d node:[%p]  child:[%p]\n",removeEdge, level, v, node,edge?edge->child:NULL);
        if (!removeEdge) {
            if (!node->branches) {

            } else if (HAS_SINGLE_CHILD(node)) {
                nnPNode child = node->branches->child;

                    if (!node->shared_content)
                        fatal("single Node without Shared content");

                    if (node->branches->shared_child) {
                        // Rprintf("SINGLE CHILD IS SHARED\n");
                        if (node->sig == SIG_NODE_REMOVED) {
                            fatal("SINGLE CHILD IS SHARED AND RELEASED\n");
                        }
                    }

                    SIG_TEST(NODE,child);
                    if (node->content_type != SHARED_CONTENT)
                        fatal("MISSED SHARED_CONTENT %d",node->content_type);

                    if (child->terminal && child->shared_content)
                        fatal("Child content is terminal and shared content");

                    if (child->c_t_n.content != node->c_t_n.content) {
                        Rprintf("Updating shared content for node [%p]\n",node);
                        nn_set_shared_content(node,child);
                    }

            } else if (node->shared_content) {

            //                 node->content_type == BETA_CONTENT
            } else if ((v != ASTERISK_VALUE) && (IS_BETA(node->content_type)) ) {
                if (node->terminal)  {
                    Rprintf("Remove from beta terminal [%p]\n",node->c_t_n.terminal);
                    terminal_remove(S->pit,node->c_t_n.terminal,obj);
                } else {
                    Rprintf("<<<<< analizing beta ...\n");
                    nn_remove_step(node->c_t_n.content,S,obj,dim+1,rlist,betas);
                    has_content = 1;
                    Rprintf(">>>>> Done analizing beta\n");
                }
            }
        } else {
            if (v == ASTERISK_VALUE) {
                if (edges[level-1]->shared_child) fatal("Primary alpha is shared");
                Rprintf("Alpha: Releasing node [%p]... ",node);
                nn_node_to_free(rlist,node,NULL);
                    Rprintf("done\n");
                nn_stats.n_alphas--;

            } else {
                removeEdge = 0;
                Rprintf("Removing edge... ");
                nn_remove_edge_child(node,edge);
                Rprintf("done\n");

                // ficou sem filhos
                if (!node->branches) {
                    if (!node->shared_content)
                        fatal("Single Node with proper content");

                    // nao remove a raiz
                    if (level==level0) {
                        node->terminal = 0;
                        node->c_t_n.content = NULL;
                        node->content_type = SHARED_CONTENT;

                    } else {
                        Rprintf("No Child: Releasing node [%p]... ",node);
                        //nn_insert_in_nodes(rlist,node);
                        SIG_TEST(NODE,node);
                        nn_node_to_free(rlist,node,NULL);
                        Rprintf("done\n");
                    }
                    removeEdge = 1;

                // tornou-se filho unico
                } else if (nn_has_single_child(node)) {
                    nnPNode child = node->branches->child;
                    int terminal = node->terminal;

                    if (!child) fatal("Missing Child\n");
                    if (terminal != child->terminal) fatal("Child content terminal problem");

                    if (node->shared_content)
                        fatal("Non single Node with Shared content");

                    if (node->branches->shared_child) {
                        Rprintf("SINGLE CHILD IS SHARED\n");
                        if (node->sig == SIG_NODE_REMOVED) {
                            fatal("SINGLE CHILD IS SHARED AND RELEASED\n");
                        }
                    }

                    SIG_TEST(NODE,child);
                    if (!IS_BETA(node->content_type))
                        fatal("MISSED BETA_CONTENT %d",node->content_type);

                    nn_stats.n_betas--;
                    if (terminal) {
                        nnPTerminal pt = node->c_t_n.terminal;
                        //fatal("TERMINAL X");
                        SIG_TEST(TERMINAL,pt);
                        Rprintf("$$$ single_child: terminal_release [%p]\n",pt);
                        if (node->c_t_n.terminal->sig == SIG_TERMINAL) {
                            terminal_release(S->pit,pt);
                        } else {
                            fatal("Invalid SIG_NODE");
                        }
                        SIG_TEST(TERMINAL,child->c_t_n.terminal);
                        Rprintf("$$$ single_child: child's content [%p]\n",child->c_t_n.content);

                    } else {
                        Rprintf("Remove As Purge... ");
                        if (nn_remove_step(node->c_t_n.content,S,obj,dim+1,rlist,betas))
                           fatal("Empty Beta");
                        Rprintf("done\n");
                        //Rprintf("do_purge [%p]\n",node->c_t_n.content);
                        //nn_insert_in_nodes(betas,node->c_t_n.content);
                        nn_do_purge(node->c_t_n.content, child->c_t_n.content, rlist,S->pit);
                        purged = 1;
                    }

                    SIG_TEST(NODE,child);
                    Rprintf("Set Shared Content [%p] [%p/%p] terminal? %d... ",node,child,child->c_t_n.content,child->terminal);
                    nn_set_shared_content(node,child);
                    if (terminal != child->terminal) fatal("Child content terminal problem 2");

                    Rprintf("done\n");

                // mais de um filho
                } else {
                    if (IS_BETA(node->content_type)) {
                        if (node->shared_content) fatal("Inconsistencia: Shared");
                        if (node->terminal) {
                            int c;
                            nnPTerminal pt = node->c_t_n.terminal;
                            SIG_TEST(TERMINAL,pt);
                            Rprintf("DECREMENT TERMINAL ... ");
                            S->pit->remove(pt,obj);
                            c = S->pit->count(pt);
                            if (c == 0) {
                                fatal("Terminal Zero");
                            }

                            //printf("REMOVE BETA TERMINAL ... ");
                            //if (pt->sig == SIG_TERMINAL)
                            //    nn_release_time_series(pt);
                            // printf("Time Series: %x  %d\n",pt->sig,pt->n);
                            //printf("done\n");
                        } else {
                            Rprintf("REMOVE STEP... ");
                            if (nn_remove_step(node->c_t_n.content,S,obj,dim+1,rlist,betas))
                                fatal("Empty Beta");
                            has_content = 1;

                            Rprintf("done\n");
                        }
                    }
                }
            }
        }

        if (purged && has_content) {
            if (!node->terminal && !node->shared_content) {
                Rprintf("Redirect removed edges\n");
                nn_redirect_purged_nodes(node->c_t_n.content);
            }
        }



        level --;
    }

    Rprintf("Exiting removeEdge: %d ...\n",removeEdge);
    if (removeEdge) {
        Rprintf("[Remove Root]");
        node->c_t_n.content = NULL;
        SIG_TEST(NODE,node);
    }
    return removeEdge;
}

/*--------------------------
*
*
*
*/
nnPNode nn_remove_from_nanocubes(nnPNode root,nnPSchema S,nnPObject obj) {
    nnNodesToFree rlist;
    static int c = 0;
    int remove_root;
    nnPNodes betas;

    rlist.first = NULL;

    if (!obj->ok)
        nn_prepare_object_values(obj,S);

    Rprintf("--------------------------------------------\n");
    Rprintf("Removing: ");
    //print_values(obj,28);

    betas = nn_create_nodes();
    remove_root = nn_remove_step(root,S,obj,1,&rlist,betas);
    // check_ref_purge(root,S,1,0,betas);
    {
        nnPNodeItem p, pn;
        for (p=betas->first; p; p=pn) {
            pn = p->next;
            //nn_do_purge(p->node,&rlist,S->pit);
            free(p);
        }
        free(betas);
    }

    if(remove_root) {
        Rprintf("=========== REMOVE ROOT ==============\n");
    }
    nn_release_nodes_to_free(&rlist);

    c++;
    //printf("Remocoes: %d\n",c);
    return root;
}



#if 0
/*--------------------------
*
*
*
*/
nnPNode nn_del_from_nano_cube(nnPNode root,nnPSchema S,int dim, nnPObject obj) {
    nnPDeleted deleted;

    if (!obj->ok)
        nn_prepare_object_values(obj,S);

    deleted = nn_create_nodes();
    root = nn_del(root,S,dim,obj,deleted);
    nn_release_nodes(deleted,1);

    return root;
}
#endif

#endif


























/******************************************************************************************//*

    procedure nn_nano_cube

*//******************************************************************************************/

#ifndef __NOVO__
/*--------------------------
*
*
*
*/
void nn_add_to_nano_cube(nnPNode nano_cube, nnPSchema S, nnPObject o) {
    nnPNodes updated_nodes;

    updated_nodes = nn_create_nodes();
    nn_add(nano_cube,o,1,S,updated_nodes);
    nn_release_nodes(updated_nodes,0);
}


#else

// $NOVO
/*--------------------------
*
*
*
*/
void nn_add_to_nano_cube_ex(nnPNode nano_cube, nnPSchema S, nnPObject o) {
    nnPNodes updated_nodes;
    nnXtree xtree;

    if(!nano_cube) return;
    if (!o->ok) {
        nn_prepare_object_values(o,S);
        //printf("            ----- \n");
        //print_values(o,28);
    }

    updated_nodes = nn_create_nodes();

    nn_xtree_prepare(&xtree,NULL,1,nano_cube);

    nn_add_ex(nano_cube,o,1,S,updated_nodes,&xtree);

    nn_release_nodes(updated_nodes,0);
}
#endif

/*--------------------------
*
*
*
*/
nnPNode nn_nano_cube(nnPClass c, nnPSchema S) {
    nnPObject o;
#ifndef __NOVO__
    nnPNodes updated_nodes;
#endif
    nnPNode nano_cube = nn_create_node();

    // varre todos os objetos da classe
    for (o = c->first; o; o=o->next) {
        trace1("-----------------------\n");

#ifdef __NOVO__
        nn_add_to_nano_cube_ex(nano_cube,S,o);
#else
        updated_nodes = nn_create_nodes();
        nn_add(nano_cube,o,1,S,updated_nodes);
        nn_release_nodes(updated_nodes,0);
#endif
    }

    return nano_cube;
}



/******************************************************************************************//*

  TOP K

*//******************************************************************************************/

typedef struct tk_bucket {
    struct tk_bucket *prev, *next;
    struct tk_monitored *first;
    int64  counter;
} tkBucket, tkPBucket;

typedef union {
  int64 i64;
} tkID;

typedef struct tk_monitored {
    struct tk_bucket    *parent;
    struct tk_monitored *next;
    tkID   id;
    int64  epsilon;
} tkMonitored, tkPMonitored;




/******************************************************************************************//*

                  Quad-Tree virtual

  Implementa uma forma compacta de registrar uma quad-tree para fins do algoritmo

  Converte GMS (Graus, Minutos, Segungos) para uma representação binaria onde cada bit
  da esquerda para direita (MSB->LSB) representa uma decisão binária:

  O valor é menor ou igual (0) ao valor médio do intervalo ou é maior (1)?
  A cada decisão o intervalo escolhido é dividido na metade e o processo é repetido.

  O algoritmo primeiro converte de GMS para centésimos de segundos (precisão aprox. de 30 cm) e
  aplica divisoes sucessivas, o que gera 27 decisões até chegar ao precisão pretendida.

  ***** Isso não afeta o número de niveis que o algoritmo do nanocube efetivamente irá usar.
  Apenas oferece uma resolução maxima.

  Note que 27 decisões permitem armazenar todas as decisões de latitude e de longitude em 54 bitas,
  o que cabe com tranquilidade em um inteiro de 64 bits.
  Se for necessário pode-se reduzir as decisões para 26 para latitude e longitude caberem em 52 bits
  e poderem ser armazenados na mantissa de um double IEEE-754.

  O paragrafo abaixo foi REVOGADO: A latitude está variando de 0 a 180 !!
  Como as latitude variam apenas entre 0 e 180 (-90 e +90), decidi nesta versão considerá-las
  variando de 0 a 360 para que a resolução fosse equivalente a da longitude. Assim, a primeira
  decisao da latitude sempre será 0 (0..180). Isso é facilmente alterável se necessário.

  A conversão dispõe as decisões de latitude nos bits pares e as de longitude nos bits impares
  para que se possa selecionar uma parte hierarquica das coordenadas usando apenas uma única
  mascara de bits.

  Nivel    01 02 03 04 05 06 07 ... 27
  Bits     01 10 11 01 11 01 10 ... 10
  Lat      0_ 1_ 1_ 0_ 1_ 0_ 1_ ... 1_
  Lon      _1 _0 _1 _1 _1 _1 _0 ... _0


*//******************************************************************************************/

#define BITS_COORD 25
#define MAX_LATITUDE 85.05
#define BIT_RESOLUTION 100

#define M_PI 3.141592634

#include <math.h>

int long2tilex(double lon, int z)
{
	return (int)(floor((lon + 180.0) / 360.0 * pow(2.0, z)));
}

int lat2tiley(double lat, int z)
{
	return (int)(floor((1.0 - log( tan(lat * M_PI/180.0) + 1.0 / cos(lat * M_PI/180.0)) / M_PI) / 2.0 * pow(2.0, z)));
}

double tilex2long(int x, int z)
{
	return x / pow(2.0, z) * 360.0 - 180;
}

double tiley2lat(int y, int z)
{
	double n = M_PI - 2.0 * M_PI * y / pow(2.0, z);
	return 180.0 / M_PI * atan(0.5 * (exp(n) - exp(-n)));
}

/*--------------------------
*
*
*
*/
#if 1
uint64 gms_to_int64_base(double g,double m, double s,int lon,int quad) {
    uint64 shift;
    uint64 mask;
    uint64 result;
    uint64 seconds;
    uint64 inicio, fim, meio;
    int i, v;
    double extra;

    inicio = 0;
    // usa centesimo de grau.
    // É um  preciosismo para postergar erros de arredondamentos
    shift = quad ? 2 : 1;
    if (lon) {
        g += 180;
        fim = 360 * 3600 * BIT_RESOLUTION;
        mask = quad? 2: 1;
    } else {
        g = MAX_LATITUDE - g;
        fim = (int) ((2*MAX_LATITUDE) * 3600 * BIT_RESOLUTION);
        mask = 1;
    }

    extra = 0; // DESLOCAMENTO / 30.87;
    seconds = (uint64) ((g * 3600 + m * 60 + s + extra) * 100); // trunca para 1 casa decimal
    result = 0;
    i = 0;
    while (inicio < fim  && i < BITS_COORD) {
        meio = (fim + inicio) / 2;
        //printf("%02d, %I64d,  %I64d,  %I64d,  %I64d",i, seconds, inicio, meio, fim);
        result <<= shift;
        if (seconds <= meio) {
            fim = meio;
            v = 0;
        } else {
            inicio = meio + 1;
            result |= mask;
            v = 1;
        }

        //printf(",  %d\n",v);
        i ++;
    }
    while (i<BITS_COORD) {
        result <<= shift;
        i++;
    }
    return result;
}


/*--------------------------
*
*
*
*/
uint64 gms_to_int64(double g,double m, double s,int lon,uint64 acc) {
    return gms_to_int64_base(g,m,s,lon,1) | acc;
}


/*--------------------------
*
*
*
*/
uint64 gms_to_int64_prev(double g,double m, double s,int lon,uint64 acc) {
    uint64 mask;
    uint64 result;
    uint64 seconds;
    uint64 inicio, fim, meio;
    int i, v;
    double extra;

    inicio = 0;
    // usa centesimo de grau.
    // É um  preciosismo para postergar erros de arredondamentos
    if (lon) {
        g += 180;
        mask = 0x2; // bits impares
//        mask = 0x000200000000000000UL; // bits impares
        fim = 360 * 3600 * 100;
    } else {
        g += 90;
        g = 180 - g; // começa no sul
        mask = 0x1; // bits pares
//        mask = 0x0001000000000000UL; // bits pares
        fim = 180 * 3600 * 100;
    }

    extra = 0; // DESLOCAMENTO / 30.87;
    seconds = (uint64) ((g * 3600 + m * 60 + s + extra) * 100); // trunca para 1 casa decimal
    result = 0;
    i = 0;
    while (inicio < fim  && i < BITS_COORD) {
        meio = (fim + inicio) / 2;
        //printf("%02d, %I64d,  %I64d,  %I64d,  %I64d",i, seconds, inicio, meio, fim);
        result <<= 2;
        if (seconds <= meio) {
            fim = meio;
            v = 0;
        } else {
            inicio = meio + 1;
            result |= mask;
            v = 1;
        }

        //printf(",  %d\n",v);
        i ++;
    }
    while (i<BITS_COORD) {
        result <<= 2;
        i++;
    }
    return result | acc;
}
#else
uint64 gms_to_int64(double g,double m, double s,int lon,uint64 acc) {
    uint64 mask;
    uint64 result;
    uint64 seconds;
    uint64 inicio, fim, meio;
    int i, v;
    double extra;

    inicio = 0;
    // usa centesimo de grau.
    // É um  preciosismo para postergar erros de arredondamentos
    if (lon) {
        g += 180;
        mask = 0x8000000000000000UL; // bits impares
//        mask = 0x000200000000000000UL; // bits impares
        fim = 360 * 3600 * 100;
    } else {
        g += 90;
        g = 180 - g; // começa no sul
        mask = 0x4000000000000000UL; // bits pares
//        mask = 0x0001000000000000UL; // bits pares
        fim = 180 * 3600 * 100;
    }

    extra = 0; // DESLOCAMENTO / 30.87;
    seconds = (uint64) ((g * 3600 + m * 60 + s + extra) * 100); // trunca para 1 casa decimal
    result = acc;
    i = 1;
    while (inicio < fim  && i < 25) {
        meio = (fim + inicio) / 2;
        //printf("%02d, %I64d,  %I64d,  %I64d,  %I64d",i, seconds, inicio, meio, fim);
        if (seconds <= meio) {
            fim = meio;
            v = 0;
        } else {
            inicio = meio + 1;
            result |= mask;
            v = 1;
        }

        //printf(",  %d\n",v);
        mask >>= 2;
        i ++;
    }
    while (mask) {
        result <<= 2;
        mask >>= 2;
    }
    return result;
}
#endif

/*--------------------------
*
*
*
*/
void coord_to_gms(double coord,double *g,  double *m, double *s) {
    double dv;
    *g = (int) coord;
    dv = (coord - *g);
    if (dv<0) dv = -dv;
    *m = (int) (dv  * 60);
    *s = ((dv  * 3600) - (60 * *m));
}


/*=============================================================================*//*

        Teste Quad-Tree

*//*=============================================================================*/

/*--------------------------
*
*
*
*/
void toBin(int64 n,char *buf) {
    uint64 mask = 0x8000000000000000UL;
    int i = 0;
    while (mask > 0) {
        buf[i] = (n & mask)? '1' : '0';
        i++;
        mask >>= 1;
    }
    buf[i] = '\0';
}

/*--------------------------
*
*
*
*/
void test_gms_to_int(void) {
    char buf[66];
    int64 acc1, acc2;

    toBin(128,buf);
    printf("%s\n",buf);

    acc1 = gms_to_int64(22,40,11,1,0);
    acc2 = gms_to_int64(-44,40,11,0,0);

    toBin(acc1,buf);
    printf("%s\n",buf);

    toBin(acc2,buf);
    printf("%s\n",buf);

    acc1 |= acc2;
    toBin(acc1,buf);
    printf("%s\n",buf);
}


void *locate_object(nnPSchema S,nnPNode root,nnPObject obj,nnPStack stack) {
    int i, j;
    nnPChain  pc;
    nnPNode node;
    nnPBranch vc;
    nnValue v;

    node = root;

    // Varre todas as chains
    for (i=0; i<S->n_chains; i++) {
        pc = S->chains[i];

        // varre todos os niveis das chains
        for (j=0; j < pc->k; j++) {
            //
            vc = node->branches;
            while (vc) {
                // verifica se os valores são iguais
                pc->lf->get_value(obj,j,&v);
                if (pc->lf->are_equal(&v,&vc->value)) {

                    // empilha o node e o branch
                    if (stack) {
                        nn_push(stack,node);         // empilha o node
                        nn_push(stack,(nnPNode) vc); // empilha o branch
                    }

                    break;
                }
                vc = vc->next;
            }
            if (!vc) return NULL;  // Não achou o objeto
            node = vc->child;

            // ultimo elemento da chain?
            if (j+1 == pc->k) {
                // chegou a time series? Então o objeto está na lista
                if (node->terminal) {
                    if (stack) {
                        nn_push(stack,node);
                    }
                    return node->c_t_n.terminal;
                }
                node = node->c_t_n.content;
            }
        }

        node = node->c_t_n.content;
    }
    return NULL;
}


void delete_object(nnPObject obj,nnPStack stack) {
    nnPBranch vc;
    nnPNode node;
    int released;
    obj;

    released = 0;
    // Trata o no dentro da time series
    //if (remove_object_from_time_series(obj)) {
    //}

    //

    while (stack->top) {
        vc = (nnPBranch) nn_pop(stack);
        node = nn_pop(stack);

        // E filho unico?
        if ( (!vc->next) && (node->branches == vc)) {

        } else {
        }
    }
}

#define NODE_WIDTH 20
#define LINE

#if 0
int draw_it(nnPNode node,int dy) {
    int nv;
    nnPBranch pb;
    int w = 0;
    if (!node->branches) return NODE_WIDTH;

    w = 0;
    for (pb = node->branches; pb; pb = pb->next) {
        w += draw_it(pb->child,dy);
    }

    // desenha o no com filhos no meio da largura dos filhos

    // desenha na posicao
    return w;
}
void draw(nnPNode root) {

}
#endif



/******************************************************************************************//*

    Time Series
    Implementa uma lista de objetos que substitui a implantação da parte temporal
    da estrutura NanoCube

*//******************************************************************************************/
typedef struct nn_object_item {
    struct nn_object_item *next;
    nnPObject object;
} nnObjectItem, *nnPObjectItem;

#ifdef __TS__

typedef struct nn_bin {
    int bin;
    int accum;
    int count;
    struct nn_bin *next;
} nnBinNode, *nnPBinNode;
#endif

#define SIG_TEST_TERMINAL(X)  if ((X)->t.sig != SIG_TERMINAL) fatal("\nSIG ERROR: line=%d SIG:[%x] @ [%p]",__LINE__,(X)->t.sig,X);

typedef struct nn_time_series {
    nnTerminal t;

    // campos especializados
    nnPObjectItem first;
#ifdef __TS__
    nnPBinNode firstBinNode;
    nnPBinNode lastBinNode;
#endif
} nnTimeSeries, *nnPTimeSeries;


static int bin_size = 86400;

#ifdef __TS__
/*--------------------------
*
*
*
*/
int binFromTime(int seconds,int base) {
    int delta = seconds - base;
    return 1 + (delta/bin_size); // bin => 10 minutos
}
#endif

/*--------------------------
**
*
*/
int nn_query_bin(nnPTerminal pt,int tm0,int tm1) {
    nnPTimeSeries ts = (nnPTimeSeries) pt;
    nnPBinNode pbn, anchor;
    nnPObject obj = ts->first->object;
    int timeBase = obj->pclass->timeBase;

    int bin0, bin1;

    bin0 = binFromTime(tm0,timeBase);

    // localiza bin0 ou maior que ele
    for (pbn = ts->firstBinNode, anchor = NULL;
         pbn && (pbn->bin < bin0);
         anchor = pbn, pbn = pbn->next);
    if (!pbn) {
        return 0;
    } else if (pbn->bin == bin0) {

    } else {

    }

    bin1 = binFromTime(tm1,timeBase);
    while (pbn && (pbn->bin <= bin1)) {
        pbn = pbn->next;
    }
    return 1;
}

/*--------------------------
*
*
*
*/
nnPBinNode nn_create_bin_node(int bin, int accum) {
    nnPBinNode pbn = (nnPBinNode) calloc(sizeof(nnBinNode),1);
    pbn->bin = bin;
    pbn->accum = accum;
    return pbn;
}

/*--------------------------
*
*
*
*/
nnPTimeSeries nn_create_time_series(void) {
    nnPTimeSeries res;
    nn_stats.n_terminal ++;
    res =  (nnPTimeSeries) calloc(sizeof(nnTimeSeries),1);
    res->t.sig = SIG_TERMINAL;
    return res;
}

/*--------------------------
*
*
*
*/
nnPTimeSeries nn_release_time_series(nnPTimeSeries ts) {
    nnPObjectItem oi,noi;
    SIG_TEST_TERMINAL(ts);
    for (oi=ts->first;oi;oi=noi) {
        noi = oi->next;
        nn_stats.n_ref_objects --;
        nn_stats.n_content--;
        free(oi);
    }
    memset(ts,0,sizeof(nnTerminal));
    ts->t.sig = SIG_TERMINAL_REMOVED;

    free(ts);

    nn_stats.n_terminal --;
#if 0
    // remove todos os bins da TS
#endif

    return NULL;
}

/*--------------------------
*
*
*
*/
nnPTimeSeries nn_clone_time_series(nnPTimeSeries ts) {
    nnPTimeSeries ts2 = nn_create_time_series();
    nnPObjectItem oi, oi2, prev;

    SIG_TEST_TERMINAL(ts);
    SIG_TEST_TERMINAL(ts2);

    ts2->t.n = ts->t.n;
    for (oi = ts->first, prev=NULL; oi; prev = oi2, oi = oi->next) {
        oi2 = (nnPObjectItem) calloc(sizeof(nnObjectItem),1);
        oi2->object = oi->object;
        nn_stats.n_content++;
        if (prev) {
            prev->next = oi2;
        } else {
            ts2->first = oi2;
        }
    }

    return ts2;
}

/*--------------------------
*
*
*
*/
int nn_count_time_series(nnPTimeSeries ts ) {
    SIG_TEST_TERMINAL(ts);
    return ts->t.n;
}

/*--------------------------
*
*
*
*/
void nn_time_series_remove(nnPTimeSeries ts ,nnPObject obj) {
    nnPObjectItem item, prev;

    SIG_TEST_TERMINAL(ts);

#if 1
    ts->t.n --;
    nn_stats.n_content --;
    nn_stats.n_ref_objects -- ;
    return;
#endif

    for (item = ts->first, prev = NULL; item; prev = item, item = item->next) {
        if (item->object == obj) {
            ts->t.n --;
            nn_stats.n_content --;
            nn_stats.n_ref_objects -- ;
            if (prev) {
                prev->next = item->next;
            } else {
                ts->first = item->next;
            }
            item->next = NULL; // desnecessario
            return;
        }
    }

#if 0
    {
        int seconds, bin;
        nnPBinNode pbn, pbn0;

        seconds = obj->pclass->getTime(obj);
        bin = binFromTime(seconds,obj->pclass->timeBase);

        pbn = pt->firstBinNode;
        if (!pbn) return;

        for (pbn0 = NULL; pbn && bin > pbn->bin; pbn0 = pbn, pbn = pbn->next) ;

        if (!pbn) return;
        if (bin < pbn->bin) {
            Rprintf("Trying t delete a inexistent object\n");
            return;
        }

        pt->n--;

        if (!pbn0) {
            if (pbn->accum == 1) goto adjust;
            pt->firstBinNode = pbn->next;
            free(pbn);
            pbn = pt->firstBinNode;
        } else {
            if (pbn0->accum == pbn->accum - 1) goto adjust;
            pbn0->next = pbn->next;
            free(pbn);
            pbn = pbn0->next;
        }
        if (!pbn) {
            pt->lastBin = NULL;
            return;
        }
adjust:
        while (pbn) {
            pbn->accum--;
            pbn->next;
        }
    }
#endif
}


/*--------------------------
*
*
*
*/
int nn_query_time_series(nnPTimeSeries ts, nnPObject what, nnPQueryInfo qi) {
    int result;
    nnPObjectItem poi;
    nnPObject po;

    // computa o valor armazenado
    if (qi->dt_i == -1) {
        return ts->t.n;
    }

    if (qi->qtype != QUERY_DATES) {
        result = 0;
        for (poi = ts->first; poi; poi = poi->next) {
            int seconds;
            int bin;

            po = poi->object;

            seconds = po->pclass->getTime(po);
            bin = binFromTime(seconds,po->pclass->timeBase);

            if (bin < qi->dt_i) continue;
            if (bin > qi->dt_f) continue;
            result ++;
        }
    } else {
        result = 0;
        for (poi = ts->first; poi; poi = poi->next) {
            int seconds;
            int bin;
            int pos;

            po = poi->object;

            seconds = po->pclass->getTime(po);
            bin = binFromTime(seconds,po->pclass->timeBase);

            if (bin < qi->dt_i) continue;
            pos = (bin - qi->dt_i) / qi->dt_bin_size;
            if (pos >= qi->dt_n_bins) continue;
            qi->databuf[pos] ++;
            result ++;
        }
    }

    return result;
}

#define __NO_CHECK_OBJECT_EXISTENCE__

#ifdef __CHECK_OBJECT_EXISTENCE__
/*--------------------------
*
*
*
*/
int nn_object_in_time_series(nnPTerminal pt,nnPObject obj) {
    nnPObjectItem item;
    for (item = pt->first; item; item = item->next) {
        if (item->object == obj) return 1;
    }

    return 0;
}
#endif

/*--------------------------
*
*
*
*/
int nn_insert_into_time_series(nnPTimeSeries ts,nnPObject obj) {
    nnPObjectItem item;

    SIG_TEST_TERMINAL(ts);
#ifdef __CHECK_OBJECT_EXISTENCE__
    if (nn_object_in_time_series(pt,obj)) {
        trace3("--- Time Series [%p] rejecting [%p]\n",pt,obj);
        return;
    }
#endif

//    trace3("--- Time Series [%p] inserting [%p]\n",pt,obj);

#if 0
    if (pt->first) {
        nnPObject po = pt->first->object;
        int s0 = po->pclass->getTime(po);
        int s1 = obj->pclass->getTime(obj);
        if (s1 < s1) return;
    }
#endif

    nn_stats.n_ref_objects ++;
    nn_stats.n_content ++;

    item = (nnPObjectItem) calloc(sizeof(nnObjectItem),1);
    item->object = obj;
    item->next = ts->first;
    ts->first = item;
    ts->t.n ++;

#if 0
    {
        int seconds, bin;
        nnPBinNode pbn;

        seconds = obj->pclass->getTime(obj);
        bin = binFromTime(seconds,obj->pclass->timeBase);

        if (!pt->firstBinNode) {
            pbn = nn_create_bin_node(bin,1);
            pt->lastBinNode = pt->firstBinNode = pbn;

        } else if (bin == pt->lastBinNode->bin) {
            pt->lastBinNode->accum ++;

        } else if (bin > pt->lastBinNode->bin) {
            // cria um novo binNode acumulando os valores do bin anterior
            pbn = nn_create_bin_node(bin,pt->lastBinNode->accum+1);

            // coloca o novo binNode no final da lista de bins
            pt->lastBinNode->next = pbn;
            pt->lastBinNode = pbn;
        }
    }
#endif
    return ts->t.n;
}


/******************************************************************************************//*

                        QUERY for Nanocubes interface

*//******************************************************************************************/

#ifdef __QUERY__


/*--------------------------
*
*
*
*/
nnPNode  nn_seek_node(nnPNode root, nnPChain chain, nnPObject obj, int zoom) {
    int i;
    //nnValue v;
    nnPNode child;
    nnPBranch vc;
    nnPNode node;
    int ival;

    if (zoom > chain->k) return NULL;

    node = root;
    qtrace("Start Seek\n");
    //aprintf("=======\n");
    for (i = 0; i < zoom; i++) {
        ival = obj->values[chain->offset+i];
        //chain->lf->get_value(obj,i+1,&v);
        child = nn_find_child_by_ivalue(node,ival,&vc);
        qtrace("Node=%p, Child=%p, I=%d, RawValue=%d, ValueStr=%s \n",node, child, i, ival,chain->lf->object_to_string(obj,i+1,trace_buf,TRACEBUF_SZ));

        // Nao encontrou uma aresta com o valor desejado
        if (!child)  return NULL;

        node = child;
    }
    qtrace("Seek found!\n");
    return node;
}


/*--------------------------
*
*
*
*/
int nn_query_object(nnPNode root, nnPSchema S, nnPObject what, int nivel, int zoom,int dt_i,int dt_f) {
    nnPNode node;
    int result = 0;

    node = root;

    if (nivel > 0) {

        // ponto
        node = nn_seek_node(root,S->chains[0],what,zoom);
        if (!node) return 0;
        nivel --;

        if (nivel > 0) {
            // vai para a raiz da arvore do proximo chain
            node = node->c_t_n.content;
            if (!node) return 0; // erro. Tem de ter content aqui

            // busca pelo valor
            node = nn_seek_node(node,S->chains[1],what,S->chains[1]->k);
            if (!node) return 0;
        }
    }

    // vai para o node que tem a time_series
    while (!node->terminal) {
        node = node->c_t_n.content;
    }

    // pega a contagem
    // deveria usar dt_i, dt_f
    result = node->c_t_n.terminal->n;

    return result;
}


/*--------------------------
*
*
*
*/
nnPTimeSeries nn_goto_time_series(nnPNode node) {
    // vai para o node que tem a time_series
    while (!node->terminal) {
        node = node->c_t_n.content;
        if (!node) {
            fatal("Node cannot reach any Time Series");
            return NULL;
        }
    }
    return (nnPTimeSeries) (node->c_t_n.terminal);
}

#include "crimes.h"

#define ON_RIGHT_QUAD(V)     (((V) & 2)>>1)
#define ON_BOTTOM_QUAD(V)    ((V) & 1)

/*--------------------------
*
*
*
*/
int nn_do_query_other_step(
        nnPNode node, nnPSchema S, nnPObject what, int nivel, nnPQueryInfo qi,
        int xi,int yi,int len,int zoom) {
    nnPBranch vc;
    int v, result;

    // o retangulo de busca esta totalmente inscrito nos limites ?
    if ( (qi->xmin <= xi) && (xi+len <= qi->xmax) &&
            (qi->ymin <= yi) && (yi+len <= qi->ymax)) {

        qtrace("INSIDE: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [zoom:%d len:%6x] [xi: %6x ] [yi: %6x]\n",
            qi->xmin, qi->xmax, qi->ymin, qi->ymax,
            zoom, len,
            xi, yi);


    // nao está?
    } else if (zoom>0) { // analisa seus quads
        int x2, y2, len2;
        int xi_new, yi_new;
        int found = 0;

        len2 = len >> 1;
        y2 = yi + len2;
        x2 = xi + len2;

        // para cada valor encontrado, desce na arvore
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;

            if (ON_RIGHT_QUAD(v)) {
                // o maximo ficou fora deste quadrante superior?
                if (qi->xmax < x2) continue; // ignora este quad
                xi_new = x2;
            } else {
                // o minimo ficou fora deste quadrante inferior?
                if (qi->xmin > x2) continue; // ignora este quad
                xi_new = xi;
            }
            if (ON_BOTTOM_QUAD(v)) {
                // o maximo ficou fora deste quadrante superior?
                if (qi->ymax < y2) continue; // ignora este quad
                yi_new =  y2;
            } else {
                // o minimo ficou fora deste quadrante inferior?
                if (qi->ymin > y2) continue; // ignora este quad
                yi_new =  yi;
            }
            found = 1;
            qtrace("REC: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [v:%d zoom:%d len:%6x] [xi: %6x -> %6x ] [yi: %6x -> %6x]\n",
                 qi->xmin, qi->xmax, qi->ymin, qi->ymax,
                 v, zoom, len,
                 xi, xi_new, yi, yi_new);

            nn_do_query_other_step(
                vc->child, S, what, nivel,qi,
                xi_new, yi_new, len2, zoom-1);
        }
        if (!found) {
            qtrace("NOT_FOUND: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [zoom:%d len:%6x] [xi: %6x xf: %6x] [yi: %6x yf: %6x]\n",
                qi->xmin, qi->xmax, qi->ymin, qi->ymax,
                zoom, len,
                xi, xi+len, yi, yi+len);
        }
        return 1;
    } else {
        qtrace("LEAF: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [zoom:%d len:%6x] [xi: %6x ] [yi: %6x]\n",
            qi->xmin, qi->xmax, qi->ymin, qi->ymax,
            zoom, len,
            xi, yi);

    }

    // !!! agora chegou ao nivel desejado da arvore

    // vai para o proxima sub-arvore
    node = node->c_t_n.content;
    nivel --;

    if (qi->qtype==QUERY_TOTAL) {
        result = nn_query_time_series(nn_goto_time_series(node),what,qi);
        qi->databuf[0] += result;

    // unico valor ?
    } else if (qi->value >= 0) {

        // valor procurado
        // fixado para crime
        int v0 = what->values[S->chains[1]->offset];

        // procura pelo valor
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;
            if (v == v0) break;
        }

        // nao achou o valor? retorna
        if (!vc) return 1;

        // pega o filho do ramo onde o valor foi encontrado
        node = vc->child;

        // computa as ocorrencias do valor
        result = nn_query_time_series(nn_goto_time_series(node),what,qi);

        if (qi->qtype == QUERY_VALUES) {
            // acumula o total na posicao 0
            qi->databuf[0] += result;

        } else if (qi->qtype == QUERY_DATES) {
            // ja foi processado em nn_query_time_series
        }

    // varios histograma de todos os valores
    } else {

        // contabiliza todos os valores dos nodes
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;

            // DEVE MUDAR para lidar com mais níveis

            // assume que esta no nivel desejado
            // ajustar para multiplos niveis
            result = nn_query_time_series(nn_goto_time_series(vc->child),what,qi);

            if (qi->qtype==QUERY_VALUES) {
                // acumula os resultados
                qi->databuf[v] += result;
                // printf("Values: path: %d  val: %d\n",v,result);
            } else {
                // ja foi processado em nn_query_time_series
            }
        }
    }

    return 1;

}

/*--------------------------
*
*
*
*/
int nn_do_query_location_step(
        nnPNode node, nnPSchema S, nnPObject what, int nivel, nnPQueryInfo qi,
        int row, int col, int n) {

    nnPBranch vc;
    int v, result;

    if (n>1) {
        n >>= 1;
        // para cada valor encontrado, desce na arvore
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;
            //qtrace("Location: new_n=%d, v=%d\n",n,v);
            //printf("%03d ON_RIGHT_QUAD = %d (%d->%d), ON_BOTTOM_QUAD=%d (%d->%d)\n",n,HAS_LON(v),col,col + HAS_LON(v) * n,HAS_LAT(v),row,row + HAS_LAT(v) * n);
            nn_do_query_location_step(
                vc->child, S, what, nivel,qi,
                row + ON_BOTTOM_QUAD(v) * n, col + ON_RIGHT_QUAD(v) * n,n);
        }
        return 1;
        #undef HAS_ROW
        #undef HAS_COL
    }

    // agora que chegou ao nivel desejado da arvore
    // verifica o que fazer
    node = node->c_t_n.content;
    nivel --;

    if (nivel) {
        // valor procurado
        int v0 = what->values[S->chains[1]->offset];

        // procura pelo valor
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;
            if (v == v0) break;
        }

        // nao achou o valor? retorna
        if (!vc) return 1;

        // pega o filho do ramo onde o valor foi encontrado
        node = vc->child;
    }

    // calcula o valor
    result = nn_query_time_series(nn_goto_time_series(node),what,qi);

    // coloca a contagem em row e col
    qi->databuf[row * qi->n_pontos + col] = result;

    return 1;
}

/*--------------------------
*
*
*
*/
int nn_do_query(nnPNode root, nnPSchema S, nnPObject what, int nivel, nnPQueryInfo qi) {
    nnPNode node;
    int result = 0;

    if (root) {

        if (qi->qtype == QUERY_LOCATION) {

            // acha base do processo
            node = nn_seek_node(root,S->chains[0],what,qi->zoom);
            if (!node) return 0;

            // faz a consulta
            nn_do_query_location_step(node,S,what,nivel,qi,0,0,qi->n_pontos);
        } else {
            int zoom;
            int len;

            // acha base do processo
            node = nn_seek_node(root,S->chains[0],what,qi->zoom_out);
            if (!node) return 0;

            len = 1 << (25 - qi->zoom_out);
            zoom = qi->zoom - qi->zoom_out;

            // faz a consulta
            nn_do_query_other_step(node,S,what,nivel,qi,
                qi->x_out,qi->y_out,len,zoom);

        }
    }

    return result;
}
#endif


/*=============================================================================*//*

       Crimes

*//*=============================================================================*/

typedef struct {
    int    tile_x;
    int    tile_y;

    double lat_g,lat_m,lat_s;
    double lon_g,lon_m,lon_s;
    int    tipo_crime;
    int    seconds_from_epoch;
} Crime;

typedef struct sCrimes {
    int       id;
    nnPObjectItem first, last;
    struct sCrimes *next;
} nnCrimes, *nnPCrimes;

static nnPCrimes crimes_first = NULL;
static nnPCrimes crimes_last = NULL;

/*--------------------------
*
*
*
*/
void crime_append_object(nnPCrimes pc, nnPObject po) {
    nnPObjectItem poi;

    // aloca e apenda o objeto na fila
    poi = (nnPObjectItem) calloc(sizeof(nnObjectItem),1);
    poi->object = po;
    if (pc->last)
        pc->last->next = poi;
    else
        pc->first = poi;
    pc->last = poi;

}


/*--------------------------
*
*
*
*/
int crime_gettime(nnPObject po) {
    Crime *c = (Crime *) (po->data);
    return c->seconds_from_epoch;
}

/*--------------------------
*
*
*
*/
void crime_latlon_get_value(nnPObject po, int pos, nnPValue pv) {
    Crime *p = (Crime *) (po->data);
    uint64 acc = 0;
    uint64 mask;
    int n;

    if (1) {
        mask = 0x1000000UL;
        if (pos>1) {
            mask >>= pos-1;
        }
        acc = p->tile_x & mask;
        acc <<= 1;
        acc |= p->tile_y & mask;
        n = BITS_COORD - pos;
        if (n) {
            acc >>= n;
        }

    } else {
        // converte a latitude para bits da quad-tree
        acc = gms_to_int64(p->lat_g,p->lat_m,p->lat_s,0,0);
        acc = gms_to_int64(p->lon_g,p->lon_m,p->lon_s,1,acc);

        // seleciona a parte que efetivamente sera utilizada
        pos --;
        mask = 0x0003000000000000UL;
        n = (BITS_COORD - 1) - pos;
        while (pos > 0) {
            mask >>= 2;
    //        mask = mask | 0xC000000000000000UL;
            pos --;
        }
        acc = acc & mask;
        while (n > 0) {
            acc >>= 2;
    //        mask = mask | 0xC000000000000000UL;
            n --;
        }

    }
    pv->i = (int) acc;
}

/*--------------------------
*
*
*
*/
int crime_latlon_equal_values(nnPValue v1, nnPValue v2) {
    return v1->i == v2->i;
}

/*--------------------------
*
*
*
*/
char *crime_latlon_value_to_string(nnPValue v,int pos, char *buf,int maxlen) {
    uint64 bits, val;
    char *map[] = { "00.", "01.", "10.", "11." };
    *buf = '\0';

    val = v->i;
    do {
        bits = ( val & 0x0003000000000000UL );
        val <<= 2;
        bits >>= 48;
        strncat(buf,map[bits],maxlen);
        pos --;
    } while (pos > 0);

    return buf;
}


/*--------------------------
*
*
*
*/
char *crime_latlon_to_string(nnPObject o,int pos, char *buf,int maxlen) {
    nnValue v;
    *buf = '\0';
    if (!o) return buf;
    crime_latlon_get_value(o,pos,&v);
    return crime_latlon_value_to_string(&v,pos,buf,maxlen);
}

/*--------------------------
*
*
*
*/
void crime_get_value(nnPObject po, int pos, nnPValue pv) {
    Crime *p = (Crime *) (po->data);

    pos;
    pv->i = p->tipo_crime;
}

/*--------------------------
*
*
*
*/
int crime_are_equal_values(nnPValue v1, nnPValue v2) {
    return v1->i == v2->i;
}

/*--------------------------
*
*
*
*/
char *crime_value_to_string(nnPValue v,int pos, char *buf,int maxlen) {
    pos;
    buf[3] = '\0';
    // ajustar
    sprintf(buf,"%02d",v->i);
    //strncat(buf,v->i == 0? "Crime0" : "Crime1",maxlen);
    return buf;
}

/*--------------------------
*
*
*
*/
char *crime_to_string(nnPObject o,int pos, char *buf,int maxlen) {
    nnValue v;
    crime_get_value(o,pos,&v);
    return crime_value_to_string(&v,pos,buf,maxlen);
}


/*--------------------------
*
*
*
*/
char *parseInt(char *p0, int *v,char sep) {
    char *p = strchr(p0,sep);
    if (!p) return NULL;
    *p = '\0';
    *v = atoi(p0);
    return p;
}

static nnPSchema crimes_S;
static nnPClass crimes_class;
static nnPNode crimes_nano_cube ;

/*--------------------------
*
*
*
*/
void printBits(int v, int n) {
    int mask = 1 << (n-1);
    while (mask) {
        printf("%c",(v&mask)?'1':'0');
        mask >>=1;
    }
}

/*--------------------------
*
*
*
*/
void printTiles(int x, int y, int n) {
    int mask = 1 << (n-1);
    int valx = 0;
    int valy = 0;
    while (mask) {
        valx = valx << 1 | ((x & mask)?1:0);
        valy = valy << 1 | ((y & mask)?1:0);
        if (500 < valx && valx < 1200)
            printf("(%d,%d) ",valx,valy);
        mask >>=1;
    }
}

/*--------------------------
*
*
*
*/
void crime_print(nnPObject o) {
    int i;
    nnValue v;
    for (i = 0; i < BITS_COORD; i++) {
        crime_latlon_get_value(o,i+1, &v);
        printf("%d ",v.i);
    }
    crime_get_value(o,i+1, &v);
    printf("- %d ",v.i);
    printf("\n");
}

/*--------------------------
*
*
*
*/
int crime_query_ex(double lat1,double lon1,double lat2,double lon2,nnPQueryInfo qi) {
    Crime c;
    nnPObject po;
    int seconds = 0;
    int nivel;
    int n;

    /*
    if (lat2 > lat1) { double x; x = lat1; lat1 = lat2; lat2 = x; }
    if (lon2 > lon1) { double x; x = lon1; lon1 = lon2; lon2 = x; }
    */
    c.tile_x = (int) lon1 << (BITS_COORD - qi->zoom);
    c.tile_y = (int) lat1 << (BITS_COORD - qi->zoom);

#if 0
    printTiles(c.tile_x,c.tile_y,BITS_COORD);
    printf("\n");
#endif

    // zoom += shift_log2(n_pontos);
    coord_to_gms(lat1,&c.lat_g,&c.lat_m,&c.lat_s);
    coord_to_gms(lon1,&c.lon_g,&c.lon_m,&c.lon_s);

    c.seconds_from_epoch = seconds;
    c.tipo_crime = qi->value;

    if (qi->qtype == QUERY_LOCATION) {
        qi->count = qi->n_pontos * qi->n_pontos;
        if (qi->count > qi->datalen) qi->count = qi->datalen;
        n = qi->count;

        nivel = (qi->value == -1)? 1: 2;

    } else {
        unsigned int mask = 0x1 << (BITS_COORD-1);
        int zoom = 0;

        int xres, yres;
        qi->ymin = lat2tiley(lat2,BITS_COORD);
        qi->ymax = lat2tiley(lat1,BITS_COORD) + 1;

        if (qi->ymin > qi->ymax) {
            int tmp = qi->ymin;
            qi->ymin = qi->ymax;
            qi->ymax = tmp;
            // printf("LAT INVERTIDO\n");
            // return 0;
        }

        qi->xmin = long2tilex(lon1,BITS_COORD);
        qi->xmax = long2tilex(lon2,BITS_COORD) + 1;

        if (qi->xmin > qi->xmax) {
            int tmp = qi->xmin;
            qi->xmin = qi->xmax;
            qi->xmax = tmp;
            // printf("LON INVERTIDO\n");
            // return 0;
        }

        c.tile_x = qi->xmin;
        c.tile_y = qi->ymin;

        // faz o xor entre as coordenadas e acha o primeiro bit divergente
        xres = qi->xmin ^ qi->xmax;
        yres = qi->ymin ^ qi->ymax;

        zoom = 1;
        while ( (!(yres & mask)) && (!(xres & mask))) {
            mask >>= 1;
            zoom ++;
        }

        // limpa o resto dos tiles originais
        while (mask) {
            unsigned int nmask = ~ mask;
            c.tile_x &= nmask;
            c.tile_y &= nmask;
            mask >>= 1;
        }

        qi->x_out = c.tile_x;
        qi->y_out = c.tile_y;

        zoom --;
        if (zoom >= qi->zoom) zoom = qi->zoom;
        // if (zoom < 0) zoom = 0;
        qi->zoom_out = zoom;

        if (qi->qtype == QUERY_VALUES) {
            if (qi->value == -1) {
                qi->count = 128;
            } else {
                qi->count = 1;
            }
            n = qi->count;
            qi->n_pontos = 1;
            nivel = (qi->value == -1)? 1: 2;

        } else if (qi->qtype == QUERY_DATES) {
            qi->count = qi->dt_n_bins;
            n = qi->count;
            qi->n_pontos = 1;
            nivel = (qi->value == -1)? 1: 2;

        } else if (qi->qtype == QUERY_TOTAL) {
            qi->count = 1;
            n = 1;
            qi->n_pontos = 1;
            nivel = 1;
        }
    }

    memset(qi->databuf,0,sizeof(int) * n);

    //
    po = nn_create_object(crimes_class,&c);

    nn_prepare_object_values(po, crimes_S);


    // printf("------------\n");
    allow_print=1;
    qtrace("Query Type = %d: Object Values =",qi->qtype);
    // print_values(po,BITS_COORD);

    nn_do_query(crimes_nano_cube, crimes_S, po, nivel, qi);
    allow_print=0;

    nn_release_object(po);
    return 1;
}

nnPTerminal ts_create(void) { return (nnPTerminal) nn_create_time_series(); }
nnPTerminal ts_clone(nnPTerminal pt) { return (nnPTerminal) nn_clone_time_series((nnPTimeSeries) pt); }
nnPTerminal ts_release(nnPTerminal pt) { return (nnPTerminal) nn_release_time_series( (nnPTimeSeries) pt); }

int  ts_insert (nnPTerminal pt,nnPObject obj) { return nn_insert_into_time_series((nnPTimeSeries) pt,obj); }
void ts_remove (nnPTerminal pt,nnPObject obj) { nn_time_series_remove((nnPTimeSeries) pt,obj); }

int  ts_count (nnPTerminal pt) { return nn_count_time_series((nnPTimeSeries) pt); }

static ITerminal iterminal = {
    //nnPTerminal (*create)   (void);
    ts_create,

    //nnPTerminal (*clone)    (nnPTerminal pt);
    ts_clone,

    //nnPTerminal (*release)  (nnPTerminal pt);
    ts_release,

    //void        (*insert)   (nnPTerminal pt,nnPObject obj);
    ts_insert,

    //void        (*remove)   (nnPTerminal pt,nnPObject obj);
    ts_remove,

    //int    (*count)    (nnPTerminal pt);
    ts_count,

    /* interface para Nanocubes */
    //int         (*query)    (nnPTerminal pt, nnPObject obj, void* query);
    NULL
} ;





//**********************************************************************************************************



/*--------------------------
*
*
*
*/
int prepare_crimes(nnPNode *nano_cube,nnPSchema *S, nnPClass *classCrime) {
    struct tm t;
    int seconds;
    nnPChain latlon_chain;
    nnPChain crime_chain;
    //nnPSchema S;
    //nnPClass classCrime;
    //nnPNode nano_cube ;

    // Cria as chains para as dimensoes
    latlon_chain = nn_create_chain(BITS_COORD,
                                   nn_create_label_function(
                                      crime_latlon_get_value,
                                      crime_latlon_equal_values,
                                      crime_latlon_to_string,
                                      crime_latlon_value_to_string));

    crime_chain = nn_create_chain(1,
                                   nn_create_label_function(
                                        crime_get_value,
                                        crime_are_equal_values,
                                        crime_to_string,
                                        crime_value_to_string));

    // Cria a label Function para time
    // lf_time = nn_create_label_function(NULL,NULL,NULL,NULL);



    // Cria o schema para o teste
    *S = nn_create_schema(2);
    nn_schema_set_chain(*S,1,latlon_chain);
    nn_schema_set_chain(*S,2,crime_chain);
    (*S)->pit = &iterminal;

    // Cria a classe
    t.tm_year = 2001-1900; t.tm_mon = 1-1; t.tm_mday = 1;
    t.tm_hour = 0; t.tm_min = 0; t.tm_sec = 0; t.tm_isdst = -1;
    seconds = (int) mktime(&t);

    {
        int s2;
        // 06/12/2013
        t.tm_year = 2013-1900; t.tm_mon = 6-1; t.tm_mday = 12;
        t.tm_hour = 0; t.tm_min = 0; t.tm_sec = 0; t.tm_isdst = -1;
        s2 = (int) mktime(&t);
        s2 /= 86400;
        s2 -= seconds/86400;
        s2 += 0;
    }


    *classCrime = nn_create_class_ex(sizeof(Crime),(*S)->n_values,(*S)->n_chains,crime_gettime,seconds);

    *nano_cube = nn_create_node();

    return 1;
}


/*--------------------------
*
*
*
*/
void load_crimes(FILE *f,nnPNode nano_cube,nnPSchema S,nnPClass classCrime,nnPCrimes pc) {
    char line[1024];
    char *p0,*p;

    double lat, lon;
    struct tm t;
    int seconds;
    int id;
    Crime c;
    nnPObject po;
    int count;

    count = 0;
    // le os pontos do arquivo
    while (fgets(line,1023,f)) {

        p0 = line;
        if (line[0]=='-' && line[1]=='-') break;
        // lat
        lat = strtod(p0,&p);
        p0 = p+1;

        // lon
        lon = strtod(p0,&p);
        p0 = p+1;

        // type
        p = parseInt(p0,&id,';');
        if (!p) return;

        // parse date
        if (!(p = parseInt(p+1,&t.tm_mday,'/'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_mon,'/'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_year,' '))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_hour,':'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_min,';'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        t.tm_mon --;
        t.tm_year -= 1900;
        t.tm_sec = 0;
        t.tm_isdst = 0;
        seconds = (int) mktime(&t);
        //printf("%02d-%02d-%02d %02d:%02d %12d  %6d\n",t.tm_year,t.tm_mon,t.tm_mday,t.tm_hour,t.tm_min,seconds,(seconds-classCrime->timeBase)/86400);

        // prepara o objeto
        coord_to_gms(lat,&c.lat_g,&c.lat_m,&c.lat_s);
        coord_to_gms(lon,&c.lon_g,&c.lon_m,&c.lon_s);

        c.tile_x = long2tilex(lon,BITS_COORD);
        c.tile_y = lat2tiley(lat,BITS_COORD);

        c.tipo_crime = id;
        c.seconds_from_epoch = seconds;
        po = nn_create_object(classCrime,&c);
#if 0
        if (count % 50 == 0) {
            int i;
            nnValue v;
            allow_print = 1;
            nn_prepare_object_values(po,crimes_S);
            print_values(po,BITS_COORD);

            printf("X = "); printBits(c.tile_x,BITS_COORD);
            printf("\n");
            printf("Y = "); printBits(c.tile_y,BITS_COORD);
            printf("\n");
            printTiles(c.tile_x,c.tile_y,BITS_COORD);
            printf("\n");

            for (i=0;i<BITS_COORD;i++) {
                crime_latlon_get_value(po,i+1,&v);
                qtrace("%02d ", v.i);
            }
            printf("\n");
            allow_print = 0;
        }
#endif
        crime_append_object(pc,po);
        xprintf("\n~~~~~~~~~~~~~~~~~~~~~~\n");
        nn_add_to_nano_cube_ex(nano_cube,S,po);
        // check_sums(crimes_nano_cube,crimes_S,1,0,po);
        count++;
    }

#if 0
    {
        int n = 1;
        for (po = crimes_class->first; po, n<30; po=po->next, n++) {
            printf("Deletando %d => ",n);
            nn_del_from_nano_cube(crimes_nano_cube,crimes_S,1,po);
            printf("\n");
        }
    }
#endif

    printf("Itens inseridos: %d\n",count);
    fclose(f);
}

/*=============================================================================*//*

       Crimes

*//*=============================================================================*/

/*--------------------------
*
*
*
*/
int nc_append_crimes(char *path) {
    int id;
    char fname[200];
    FILE *f;
    nnPCrimes pc;

    if (!crimes_last) {
        id = 1;
    } else {
        id = crimes_last->id + 1;
    }
//    path = "data";

    sprintf(fname,"%s\\crime50k_%06d.txt",path,id);
    fopen_s(&f,fname,"rt");
    if (!f) return(0);

    pc = (nnPCrimes) calloc(sizeof(nnCrimes),1);
    pc->id = id;

    // encadeia no final da lista
    if (crimes_last)
        crimes_last->next = pc;
    else
        crimes_first = pc;
    crimes_last = pc;

    printf("---------------------------------------------------------\n");
    printf("Inserindo itens do arquivo: %s\n",fname);
    load_crimes(f,crimes_nano_cube,crimes_S,crimes_class,pc);

    return 1;
}


/*--------------------------
*
*
*
*/
void nc_remove_crimes(char *path) {
    char fname[200];
    int i;
    //int id;
    //FILE *f;

    nnPCrimes pc = crimes_first;
    nnPObjectItem poi;

    if (!pc) return;

    if (!pc->first) fatal("Erro 120\n");

    sprintf(fname,"%s\\crime50k_%06d.txt",path,pc->id);
    //fopen_s(&f,fname,"rt");
    //if (!f) return(0);

    printf("---------------------------------------------------------\n");
    //check_sums(crimes_nano_cube,crimes_S,1,0,NULL);
    printf("Removendo lotes de itens do arquivo %s\n",fname);
    i = 0;
    do {
        poi = pc->first;
        pc->first = poi->next;
        if (!pc->first) pc->last = NULL;

        if (!poi) fatal("Erro 110\n");

        Rprintf("I=%d\n",i);i++;
        crimes_nano_cube = nn_remove_from_nanocubes(crimes_nano_cube,crimes_S,poi->object);
        check_sums(crimes_nano_cube,crimes_S,1,0,poi->object);
        free(poi);
        //break;
    } while (pc->first);

#ifdef __STATS__
    show_stats();
#endif

    // passa para o proximo crimes
    crimes_first = pc->next;
    if (!crimes_first) crimes_last = NULL;
    free(pc);
}


/*--------------------------
*
*
*
*/
void enqueue_crimes(nnPCrimes pc) {
    if (!crimes_last) {
        crimes_first = pc;
    }
    pc->next = crimes_last;
    crimes_last = pc;
}

/*--------------------------
*
*
*
*/
void dequeue_crimes(void) {
    nnPCrimes pc;

    if (!crimes_first) return;
    pc = crimes_first;
    crimes_first = pc->next;
    if (!crimes_first) crimes_last = NULL;
}

#define NFILES_IN 10
#define NFILES_OUT 1

/*--------------------------
*
*
*
*/
void carrega_crimes(char *path) {
    int n = NFILES_IN;
    clock_t start;
    int ms;
    long int mem;
    char *suf = "Bytes";

    start = clock();
    while (n) {
        nc_append_crimes(path);
        n--;
    }
    ms = (clock()-start);

    mem = nn_stats.n_ref_objects * sizeof(nnObjectItem) ;
    mem += nn_stats.n_objects * (crimes_class->size);

    if (mem > 10024) {
        mem /= 1024;
        suf = "KB";
    }

    if (mem > 10024) {
        mem /= 1024;
        suf = "MB";
    }

    printf("---------------------------------------\nAfter load:\n");
    printf("Tempo (ms): %d \n",ms);
#ifdef __STATS__
    printf("---------------------------------------\n");
    show_stats();
#endif


//    load_crimes(fname1,crimes_nano_cube,crimes_S,crimes_class);
//    load_crimes(fname2,crimes_nano_cube,crimes_S,crimes_class);

#ifdef __OLD_DUMP__
      smart_dump(nano_cube,S,"");
#endif
}

/*--------------------------
*
*
*
*/
void descarrega_crimes(char *path) {
    int n = NFILES_OUT;
    clock_t start;
    int ms;
    long int mem;
    char *suf = "Bytes";

    Rprintf("Descarregando a estrutura de dados ...\n");
    start = clock();
    while (n) {
        nc_remove_crimes(path);
        n--;
    }
    ms = (clock()-start);

    mem = nn_stats.n_ref_objects * sizeof(nnObjectItem) ;
    mem += nn_stats.n_objects * (crimes_class->size);

    if (mem > 10024) {
        mem /= 1024;
        suf = "KB";
    }

    if (mem > 10024) {
        mem /= 1024;
        suf = "MB";
    }

    printf("---------------------------------------\nAfter unload:\n");
    printf("Tempo (ms): %d\n",ms);
#ifdef __STATS__
    printf("---------------------------------------\n");
    show_stats();
#endif


//    load_crimes(fname1,crimes_nano_cube,crimes_S,crimes_class);
//    load_crimes(fname2,crimes_nano_cube,crimes_S,crimes_class);

#ifdef __OLD_DUMP__
      smart_dump(nano_cube,S,"");
#endif
}

#define __TESTE__

/*--------------------------
*
*
*
*/
void crime_init(int bin) {
    char *dir = "data";
    memset(&nn_stats,0,sizeof(nn_stats));

    bin_size = 3600;

    printf("Inicializando a estrutura de dados ...\n");
    if (!prepare_crimes(&crimes_nano_cube,&crimes_S,&crimes_class)) {
        printf("Failing to prepare\n");
        return;
    };

#ifndef __TESTE__
    carrega_crimes(dir);
#else
    carrega_crimes(dir);
    check_sums(crimes_nano_cube,crimes_S,1,0,NULL);

    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);

    carrega_crimes(dir);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);
    check_sums(crimes_nano_cube,crimes_S,1,0,NULL);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);

#if 0
    carrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    carrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
#endif
    printf("done\n");
    exit(0);
#endif


//        carrega_crimes("ponto1.csv","ponto2.csv");
}

/*--------------------------
*
*
*
*/
void main__(int argc, char *argv[] ) {
    crime_init(86400);
}
/******************************************************************************************//*

    NanoCubes for Real-Time Exploration os Spatiotemporal Datasets

    Nilson L. Damasceno

*//******************************************************************************************/

/*---------------------------------------------------------------------------------------*/

#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <malloc.h>
#include <time.h>

#include <setjmp.h>

#include "nn.h"

#define __NOVO__
#define __WITH_DUMP__0
#define __OLD_DUMP__0

#define __TS__
#define __QUERY__

#include <string.h>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

#ifdef _WIN32
#define PATH_SEP '\\'
#else
#define PATH_SEP '/'
#endif

#define MAXPATH 1024
static char path[MAXPATH];
static char tmp[MAXPATH];

#define LOOP \
    __pragma(warning(push)) __pragma(warning(disable:4127)) \
    while (1)  __pragma(warning(pop))

/******************************************************************************************//*

    Facilidades de trace e debug

*//******************************************************************************************/

#define SIG_TEST(W,X)  if ((X)->sig != SIG_##W) fatal("\nSIG ERROR: line=%d SIG:[%x] @ [%p]",__LINE__,(X)->sig,X);


#define trace1(A)
//tracef0(A)
#define trace2(A,B) tracef0(A,B)

#define trace3(A,B,C)
//tracef0(A,B,C)

#include <stdarg.h>

#define TRACEBUF_SZ 518
static char trace_buf[TRACEBUF_SZ];

void tracef0(char *fmt,...) {
    va_list args;

    fmt;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

int allow_print;

void qtrace(char *fmt,...) {
    va_list args;

    if (allow_print != 2) return;

    fmt;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}


// para remocao
void xprintf(char *fmt,...) {
#if 1
    fmt;
#else
    va_list args;
    fmt;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
#endif
}


// para remocao
void Rprintf(char *fmt,...) {
#if 0
    fmt;
#else
    va_list args;
    fmt;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    fflush(stdout);
#endif
}



// para adicao
void Iprintf(char *fmt,...) {
#if 0
    fmt;
#else
    va_list args;
    fmt;

    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
#endif
}

void error(char *fmt,...) {
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr,fmt, args);
    va_end(args);
}

static int recover = 0;
static jmp_buf jmp;

static jmp_buf *fatal_jmp = NULL;

static void  set_fatal_mode(jmp_buf *jmp ) {
    fatal_jmp = jmp;
}

void fatal(char *fmt,...) {
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr,fmt, args);
    va_end(args);
    fflush(stdout);

    if (fatal_jmp) longjmp(*fatal_jmp,10);
    exit(100);
}


static struct {
    int n_ref_objects;
    int n_objects;
    int n_iguais;
    int n_terminal;
    int n_nodes;
    int n_content;
    int n_alphas;
    int n_betas;
    int n_edges;
} nn_stats;

#define __STATS__

#ifdef __STATS__
/*--------------------
*
*
*
*/
void show_stats(void) {
    return;
    printf("n_alphas %d\n",nn_stats.n_alphas);
    printf("n_betas %d\n",nn_stats.n_betas);
    printf("n_terminal %d\n",nn_stats.n_terminal);
    printf("n_nodes %d\n",nn_stats.n_nodes);
    printf("n_objects %d\n",nn_stats.n_objects);
    printf("n_edges %d\n",nn_stats.n_edges);
    printf("n_iguais %d\n",nn_stats.n_iguais);
//    printf("n_content %d\n",nn_stats.n_content);
//    printf("n_ref_objects %d\n",nn_stats.n_ref_objects);
}
#endif

/******************************************************************************************//*

    Tipos utilizados que podem exigir adaptações dependendo do compilador

*//******************************************************************************************/
typedef __int64          int64;
typedef unsigned __int64 uint64;
typedef unsigned int     uint;
typedef unsigned char    uchar;
typedef signed char      schar;

#pragma pack(8)

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


/******************************************************************************************//*

    nnObject, nnClass

    Assume que todos os objetos a serem indexados são estruturalmente iguais
    Note que os objetos poderiam ter estruturas completamente diferentes
    sendo similares apenas pelas funções, como duck typing.

*//******************************************************************************************/

#define SIG_OBJECT  0x48282134

typedef struct nn_object {
  int sig;
  struct nn_class  *pclass;          // Classe do Objeto, ver abaixo
  struct nn_object *next;            // encadeamento na lista de todos os objetos
                                     // Nao é necessário para o algoritmo original
                                     // Mas é util para varrer todos os objetos
  // $NOVO
  int              ok;
  int              n_values;
  int              *values;         // valores resultantes do mapeamento d

  uchar             data[1];         // area de armazenamento dos dados
                                     // poderia ser data[0], mas pode não ser portável entre compiladores
}  nnObject, *nnPObject;

#ifdef __TS__
typedef int (*nnTimeFromObject) (nnPObject);
#endif

typedef struct nn_class {
    int size;                    // Tamanho dos objetos da classe
    int n_values;
    int n_chains;

#ifdef __TS__
    nnTimeFromObject getTime;
    int timeBase;
#endif

    int count;                   // Numero de objetos instanciados (util)
    nnPObject first, last;       // lista encadeada para objetos da classe
                                 // Nao e necessario mas eh util
} nnClass, *nnPClass;

/*--------------------
*
*
*/
#ifdef __TS__
nnPClass nn_create_class_ex(int size, int n_values,int n_chains,nnTimeFromObject getTime,int timeBase) {
#else
nnPClass nn_create_class(int size, int n_values) {
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
nnPClass nn_create_class(int size, int n_values,int n_chains) {
    return nn_create_class_ex(size,n_values,n_chains,NULL,0);
}


/*--------------------
*
*
*/
nnPObject nn_create_object(nnPClass pclass,void *data) {
    nnPObject po;
    int size;

    // @@@@ Poderia haver um pool de objetos para reaproveitar alocacao

    // aloca um objeto
    size = sizeof(nnObject) + pclass->size;
    size -= sizeof(uchar);  // descarta o byte usado no struct por conta da compatibilidade
    po = (nnPObject) calloc(1,size);
    nn_stats.n_objects ++;

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
nnPObject nn_release_object(nnPObject o) {
    SIG_TEST(OBJECT,o);
    // @@@@ faltam mecanismos de defesa
    nn_stats.n_objects --;
    free(o);
    return NULL;
}

#ifdef __QUERY__

/*--------------------
*
*
*/
nnPObject nn_assign_object(nnPObject po,void *data) {
    if (!po) return NULL;
    SIG_TEST(OBJECT,po);

    // copia os dados para a area de armazenamento
    memcpy(po->data, data, po->pclass->size);

    return po;
}

#endif


/******************************************************************************************//*

    nnValue

    Union que armazena "todos" os tipos de valores possíveis.
    Não tem o tag identificador do tipo porque as operações de LabelFunction
    correspondentes ao dominio/chain "entendem" o que está armazenado.

*//******************************************************************************************/
typedef union {
//    int64    i64;
//    uint64   ui64;
    int      i;
    uint     ui;
    void     *ptr;
    double  val;
} nnValue, *nnPValue;

/*---------------------------------------------------------------------------------------*//*
   Tipos dos Ponteiros para Funções que manipulam nnValue
*//*---------------------------------------------------------------------------------------*/

// O parametro pos indica a posicao/indice na chain.
// Ele e utilizado para multiplexar todas as LabelFunctions da Chain em uma unica
// LabelFunction
typedef     void    (*nnGetValue)     (nnPObject po,  int pos, nnPValue pv);

// Retorna 0 se os 2 valores são diferentes e 1 se são iguais.
typedef     int     (*nnAreEqualValues)(nnPValue pv1, nnPValue pv2);

// Converte um objeto para string (apenas para trace)
typedef      char *  (*nnObjectToString)      (nnPObject po, int pos, char *buf, int maxlen);

// Converte um valor para string (apenas para trace)
typedef      char *  (*nnValueToString)      (nnPValue v, int pos, char *buf, int maxlen);


/******************************************************************************************//*

    nnLabelFunction

*//******************************************************************************************/

typedef struct {
  nnGetValue         get_value;
  nnAreEqualValues   are_equal;

  nnObjectToString   object_to_string;
  nnValueToString    value_to_string;
} nnLabelFunction, *nnPLabelFunction;

static void default_get_value(nnPObject o, int pos, nnPValue v) {
    o; pos;
    v->i = 0;
}

static int default_equal_values(nnPValue pv1, nnPValue pv2) {
    return pv1->i == pv2->i;
}

static char * default_object_to_string(nnPObject o, int pos, char *buf, int maxlen) {
    o; pos; maxlen;
    *buf = '\0';
    return buf;
}

static char * default_value_to_string(nnPValue v, int pos, char *buf, int maxlen) {
    v; pos; maxlen;
    *buf = '\0';
    return buf;
}

/*--------------------
*
*
*/
nnPLabelFunction nn_create_label_function(  nnGetValue get_value,
                                            nnAreEqualValues are_equal,
                                            nnObjectToString object_to_string,
                                            nnValueToString value_to_string) {
    nnPLabelFunction lf = (nnPLabelFunction) calloc(sizeof(nnLabelFunction),1);

    lf->get_value = get_value ? get_value : default_get_value;
    lf->are_equal = are_equal ? are_equal : default_equal_values;
    lf->object_to_string = object_to_string? object_to_string : default_object_to_string;
    lf->value_to_string = value_to_string? value_to_string : default_value_to_string;

    return lf;
}

/*--------------------
*
*
*/
nnPLabelFunction nn_release_label_function(nnPLabelFunction lf) {
    free(lf);
    return NULL;
}

/******************************************************************************************//*

    nnChain

*//******************************************************************************************/

typedef struct nn_chain {
    int k;                    // numero de elementos na chain
    nnPLabelFunction lf;      // label function generica
    int offset;               // deslocamento no vetor de valores
} nnChain, *nnPChain;

/*--------------------
*
*
*/
nnPChain nn_create_chain(int number_of_levels, nnPLabelFunction lf) {
    nnPChain chain = (nnPChain) calloc(sizeof(nnChain),1);
    chain->k = number_of_levels;
    chain->lf = lf;
    return chain;
}

/*--------------------
*
*
*/
nnPChain nn_release_chain(nnPChain chain) {
    // @@@@ talvez seja o caso de dar free na lf. Mas tem de ser melhor analisado porque.
    // diferentemente da linguagem Rusty, nao existem controles sobre ponteiros que
    // compartilham apontamentos

    // nn_release_label_function(chain->lf);
    free(chain);
    return NULL;
}


/******************************************************************************************//*

    ITerminal

*//******************************************************************************************/

#define SIG_TERMINAL  0x44219333
#define SIG_TERMINAL_REMOVED 0x7A289818

typedef struct nn_terminal {
    int sig;
    int n;
} nnTerminal, *nnPTerminal;

typedef  nnPTerminal (*TerminalCreate)   (void);
typedef  nnPTerminal (*TerminalClone)    (nnPTerminal pt);
typedef  nnPTerminal (*TerminalRelease)  (nnPTerminal pt);
typedef  int         (*TerminalInsert)   (nnPTerminal pt,nnPObject obj);
typedef  void        (*TerminalRemove)   (nnPTerminal pt,nnPObject obj);
typedef  int         (*TerminalCount)    (nnPTerminal pt);
typedef  int         (*TerminalQuery)    (nnPTerminal pt, nnPObject obj, void* query);

typedef struct {
    /* interface mandatória */
    TerminalCreate      create;
    TerminalClone       clone;
    TerminalRelease     release;
    TerminalInsert      insert;
    TerminalRemove      remove;
    TerminalCount       count;

    /* interface para Nanocubes */
    TerminalQuery       query;
} ITerminal, *PITerminal;


nnPTerminal terminal_create(PITerminal pit) {
    //nn_stats.n_terminal ++;
    return pit->create();
}

nnPTerminal terminal_clone(PITerminal pit, nnPTerminal pt) {
    //nn_stats.n_terminal += pit->count(pt);
    return pit->clone(pt);
}

nnPTerminal terminal_release(PITerminal pit, nnPTerminal pt) {
    //nn_stats.n_terminal --;
    return pit->release(pt);
}

int terminal_insert (PITerminal pit, nnPTerminal pt, nnPObject obj) {
    return pit->insert(pt,obj);
}

void terminal_remove (PITerminal pit, nnPTerminal pt, nnPObject obj) {
    pit->remove(pt,obj);
}

int  terminal_count (PITerminal pit, nnPTerminal pt) {
    return pit->count(pt);
}


#ifdef __NOVO__


/******************************************************************************************//*

     nnSchema

*//******************************************************************************************/

typedef struct nn_schema {
    int       n_values;
    PITerminal pit;            // ponteiro para interface de terminal

    int       n_chains;        // assert(n_chains <= pnc->n)
    nnPChain  chains[1];       // contem todas as chains planejadas para o schema
                               // deveria ser chains[0], mas não eh por portabilidade
} nnSchema, *nnPSchema;

/*--------------------
*
*
*/
nnPSchema nn_create_schema(int number_of_chains) {
    nnPSchema s;
    int size;

    size = sizeof(nnSchema); //
    size += (number_of_chains * sizeof(nnChain)); // @@@@ esta alocando uma nnPChain a mais, mas ok
    s = (nnPSchema) calloc(size,1);

    s->n_values = 0;
    s->n_chains = number_of_chains;
    return s;
}

/*--------------------
*
*
*/
int nn_schema_set_chain(nnPSchema ps,int i,nnPChain chain) {
    int offset;

    if (i < 1) return 0;     // @@@@ erro severo.
    if (i > ps->n_chains) return 0; // @@@@ erro severo. Ultrapassou o numero de chains
    ps->chains[i-1] = chain;

    // $NOVO
    // ajusta o deslocamento
    offset = 0;
    ps->n_values = 0;
    for (i=0; i < ps->n_chains; i++) {
        xprintf("Chain %d Offset %d\n",i,offset);
        if (!ps->chains[i]) continue;
        ps->chains[i]->offset = offset;
        ps->n_values +=  ps->chains[i]->k;
        offset = offset + ps->chains[i]->k + 1;
    }

    return 1;
}

/*--------------------
*
*
*/
nnPSchema nn_release_schema(nnPSchema schema) {
#if 0
    // @@@@ deveria desalocar os chains alocados? provavelmente
    // Desaloca as chains alocadas
    int i;
    for (i = 0; i < schema->n; i++) {
        nn_release_chain(schema->chains[i]);
    }
#endif
    free(schema);
    return NULL;
}


/******************************************************************************************//*

    nnNode e nnBranch

    nnBranch armazena uma "aresta" que interliga um
    node pai e um node filho baseado em um valor.

    Cada node Pai tem uma lista de nnBranch, onde cada elemento tem
    um valor e um child, onde esse child pode ou não ser SHARED

*//******************************************************************************************/

#define HAS_CHILD(X) ((X)->branches)
#define HAS_NO_CHILD(X) ((X)->branches == NULL)
#define HAS_SINGLE_CHILD(X) (HAS_CHILD(X) && ((X)->branches->next == NULL))
#define HAS_MULTI_CHILD(X) (HAS_CHILD(X) && ((X)->branches->next != NULL))

#ifdef __NOVO__
#define SHARED_CONTENT        0
#define ALPHA_CONTENT         1
#define BETA_CONTENT          2
#define OMEGA_CONTENT         4
#define BETA_OMEGA_CONTENT    (BETA_CONTENT | OMEGA_CONTENT)

#define IS_ALPHA(NODE)           ((NODE) & ALPHA_CONTENT)
#define IS_BETA(NODE)            ((NODE) & BETA_CONTENT)
#define IS_OMEGA(NODE)           ((NODE) & OMEGA_CONTENT)

#endif

#define SIG_NODE         0x76234102
#define SIG_NODE_REMOVED 0x84391345

typedef struct nn_node {
    int sig;
    struct nn_branch *branches;

#ifdef __NOVO__
    int               counter;    // registra quantos objetos/paths esta "time_series" acumula
#else
    union {
        struct nn_branch *branches;   // lista de valores deste node
    } info;
#endif

#ifdef __NOVO__
    int content_type;    // $NOVO Vai substituir os itens abaixo
#endif
    int shared_content;  // Indica que o conteudo e compartilhado
    int terminal;        // Indica que o conteudo e uma Time Series

    //
    int dimension;       // apenas para o dump

    union {
        struct nn_node        *content;
        struct nn_terminal    *terminal;
        struct nn_node        *next;
    } c_t_n;

} nnNode, *nnPNode;

typedef struct {
    nnPNode first;
} nnNodesToFree, *nnPNodesToFree;

#define SIG_BRANCH  0x63829153

typedef struct nn_branch {
    struct nn_branch *next;

    nnValue         value;
    nnPNode         child;
    int             shared_child;
    int    sig;
} nnBranch, *nnPBranch;


/*--------------------------
*
*
*
*/
nnPNode nn_create_node(void) {
    nnPNode res =  (nnPNode) calloc(sizeof(nnNode),1);
    res->sig = SIG_NODE;
    nn_stats.n_nodes++;
    return res;
}

/*--------------------------
*
*
*
*/
nnPNode nn_release_node(nnPNode node) {
    SIG_TEST(NODE,node);
    node->branches = NULL;
    nn_stats.n_nodes--;
    free(node);
    return NULL;
}


/*--------------------------
*
*
*
*/
nnPNode nn_node_to_free(nnPNodesToFree list, nnPNode node, nnPNode redirect) {
    SIG_TEST(NODE,node);
    memset(node,0xCC,sizeof(nnNode));
    node->sig = SIG_NODE_REMOVED;
    node->c_t_n.next = list->first;

    node->branches = (nnPBranch) redirect;   // Salva o no previo para recupe

    list->first = node;
    return node;
}



/*--------------------------
*
*
*
*/
nnPBranch nn_create_value_child(void) {
    nnPBranch vc =  (nnPBranch) calloc(sizeof(nnBranch),1);
    vc->sig = SIG_BRANCH;
    nn_stats.n_edges ++;
    return vc;

}

/*--------------------------
*
*
*
*/
nnPBranch nn_release_value_child(nnPBranch p) {
    SIG_TEST(BRANCH,p);
    nn_stats.n_edges--;
    memset(p,0,sizeof(nnBranch));
    free(p);
    return NULL;
}


/******************************************************************************************//*


*//******************************************************************************************/


/*--------------------------
*
*
*
*/
void nn_release_nodes_to_free(nnPNodesToFree rlist) {
    nnPNode next, node;

    // esvazia a lista de nodes coletados
    for (node = rlist->first; node; node = next) {
        Rprintf("Releasing [%p]",node);
        next = node->c_t_n.next;
        SIG_TEST(NODE_REMOVED,node);
        node->sig = SIG_NODE;
        nn_release_node(node);
    }
    Rprintf("\n");
}




/******************************************************************************************//*

    nnNodes
    Implementa a lista updated_nodes e os "metodos" necessarios a ela
    INSERT in Nodes e NODE IN NODES

*//******************************************************************************************/

typedef struct nn_node_item {
    struct nn_node_item *next;
    nnPNode node;
} nnNodeItem, *nnPNodeItem;

typedef struct {
    nnPNodeItem first;
} nnNodes, *nnPNodes;

typedef nnPNodes nnPDeleted;

/*--------------------------
*
*
*
*/
nnPNodes nn_create_nodes(void) {
    return (nnPNodes) calloc(sizeof(nnNodes),1);
}

/*--------------------------
*
* Poderia se beneficiar de um pool de nnNodeItem
*
*/
void nn_release_nodes(nnPNodes nodes,int deep) {
    nnPNodeItem p, pn;
    for (p=nodes->first; p; p=pn) {
        pn = p->next;
        if (deep) nn_release_node(p->node);
        free(p);
    }
    free(nodes);
}

/*--------------------------
*
*
*
*/
int nn_node_in_nodes(nnPNodes nodes,nnPNode node) {
    nnPNodeItem item;
    for (item = nodes->first; item; item=item->next) {
        if (item->node == node) return 1;
    }
    return 0;
}

/*--------------------------
*
*
*
*/
void  nn_insert_in_nodes(nnPNodes nodes,nnPNode node) {
    nnPNodeItem item;

    item = (nnPNodeItem) calloc(sizeof(nnNodeItem),1);
    item->node = node;
    item->next = nodes->first;
    nodes->first = item;
}

// $NOVO
/*--------------------------
*
*
*
*/
void  nn_append_to_nodes(nnPNodes nodes,nnPNode node) {
    nnPNodeItem item;

    for (item = nodes->first; item; item = item->next) {
        if (!item->next) break; // para no ultimo item da lista
    }

    if (!item) {
        item = (nnPNodeItem) calloc(sizeof(nnNodeItem),1);
        nodes->first = item;
    } else {
        item->next = (nnPNodeItem) calloc(sizeof(nnNodeItem),1);
        item = item->next;
    }
    item->node = node;
    item->next = NULL;
}

/******************************************************************************************//*

    Tree

*//******************************************************************************************/

#define OP_NONE          0
#define OP_FORK          1    // sinaliza que foi criado um novo ramo
#define OP_SHARE         2    // sinaliza que foi criado um shared child
#define OP_DESHARE       3    // sinaliza que um shared child foi separado
#define OP_RESHARE       4    // sinaliza que um share foi reapontado
#define OP_SKIP          5    // sinaliza que foi criado um shared child
#define OP_DOUBLE        6    // sinaliza que um terminal agora tem 2 valores

#define MAX_PATH  200
typedef struct s_xtree {
    int     done;
    int     op;                // indica a primeira operacao ocorreu na arvore
    int     n;
    nnPNode path[MAX_PATH];    // armazena o caminho para esta aresta
    struct  s_xtree *src;      // Xtree
    nnPNode root;
} nnXtree, *nnPXtree;


/*--------------------------
*
*
*
*/
void nn_xtree_inherits_rest(nnPXtree xtree,int level) {
    int i;
    if (!xtree->src) return;
    for (i = 0; i<xtree->n; i++) {
        xtree->path[i] = xtree->src->path[i];
    }
}


/*--------------------------
*
*
*
*/
nnPXtree nn_xtree_prepare(nnPXtree xtree,nnPXtree src,int level,nnPNode root) {
    level = level;

    xtree->src = src;
    xtree->done = 0;
    xtree->op = OP_NONE;
    xtree->root = root;
    // pode ser otimizado
    if (!src) memset(xtree->path,0,MAX_PATH * sizeof(nnPNode));

    return xtree;
}



// $NOVO
typedef struct  s_head {
        nnPNode node;         // no o node que sofreu a operacao
        int     dimension;    // dimensão (chain) onde a operacao ocorreu.
        int     height;       // altura onde a operacao ocorreu.
        nnPBranch vc;         // Branch para o head.node
        int value_index;
} nnHead, *nnPHead;

typedef struct s_tree {
    int     op;                // indica a primeira operacao ocorreu na arvore
    int     done;

    nnHead head;

    int     incr;              // incremento
    int     base_dim;          // dimensao base da arvore
    struct  s_tree *src;       // arvore anterior
//    int     changed;           // flag de modificacao
//    int     forked;         //
} nnTree, *nnPTree;



/*--------------------------
*
*
*
*/
nnPTree nn_init_tree(nnPTree tree,int base_dimension,nnPTree last_tree) {
    memset(tree,0,sizeof(nnTree));
    if (last_tree) {
        memcpy(&tree->head,&last_tree->head,sizeof(nnHead));
        tree->src = last_tree;
    }
    // tree->op = OP_NONE;
    tree->base_dim = base_dimension;

    return tree;
}


/*--------------------------
*
*
*
*/
nnPTree nn_create_tree(void) {
    nnPTree tree = (nnPTree) calloc(sizeof(nnPTree),1);
    return tree;
}


/*--------------------------
*
*
*
*/
nnPTree nn_release_tree(nnPTree tree) {
    free(tree);
    return NULL;
}


/******************************************************************************************//*

    Stack para Nodes

*//******************************************************************************************/

typedef struct nn_stack_item {
    nnPNode pn;
    int     dimension;   // $NOVO
    void    *ptr;        // ptr
    struct nn_stack_item *next;
} nnStackItem, *nnPStackItem;

typedef struct {
    nnPStackItem top;
} nnStack, *nnPStack;

/*--------------------------
*
*
*
*/
nnPStack nn_create_stack(void) {
    return (nnPStack) calloc(sizeof(nnStack),1);
}


/*--------------------------
*
*
*
*/
void nn_push(nnPStack ps, nnPNode node) {
    nnPStackItem item = (nnPStackItem) calloc(sizeof(nnStackItem),1);
    item->next = ps->top;
    item->pn = node;
    ps->top = item;
}

/*--------------------------
*
*
*
*/
nnPNode nn_pop(nnPStack stack) {
    nnPStackItem item = stack->top;
    nnPNode pn;
    if (!item) return NULL;  // @@@@ Excecao!!!
    stack->top = item->next;
    pn = item->pn;
    free(item);
    return pn;
}

// $NOVO
/*--------------------------
*
*
*
*/
void nn_push_ex(nnPStack ps, nnPNode node,int dimension,void *ptr) {
    nnPStackItem item = (nnPStackItem) calloc(sizeof(nnStackItem),1);
    item->next = ps->top;
    item->pn = node;
    item->ptr = ptr;
    item->dimension = dimension;
    ps->top = item;
}

// $NOVO
/*--------------------------
*
*
*
*/
nnPNode nn_pop_ex(nnPStack stack,int *dimension,void **ptr) {
    nnPStackItem item = stack->top;
    nnPNode pn;
    if (!item) return NULL;  // @@@@ Excecao!!!
    stack->top = item->next;
    *dimension = item->dimension;
    *ptr = item->ptr;
    pn = item->pn;
    free(item);
    return pn;
}



/*--------------------------
*
*
*
*/
int nn_stack_is_empty(nnPStack stack) {
    return ! stack->top;
}

/*--------------------------
*
*
*
*/
nnPStack nn_clear_stack(nnPStack ps) {
    // $NOVO
    while (!nn_stack_is_empty(ps)) nn_pop(ps);
    return ps;
}

/*--------------------------
*
*
*
*/
nnPStack nn_release_stack(nnPStack ps) {
    // $NOVO
    nn_clear_stack(ps);

    free(ps);
    return NULL;
}


/******************************************************************************************//*

    Funcoes auxiliares para montar o NanoCube

*//******************************************************************************************/

// funcoes auxiliares para smart_dump
int print_stats_flag = 0;
#ifdef __MAP__
int register_root(int dim,nnPNode node);
char *map_root_ex(int dim,nnPNode node);
char *map_value(nnPBranch vc);
#endif

#define ASTERISK_VALUE  (-9)

/*--------------------------
*
*
*
*/
void print_values(nnPObject po,int max) {
    int  i;
    if (max == -1) {
        max = 28;
    }
    for (i=0; i< max; i++) {
        printf("%02d ",po->values[i]);
    }
    printf("\n");
}


/*--------------------------
*
*
*
*/
void nn_prepare_object_values(nnPObject o,nnPSchema S) {
    int d, i;
    nnPChain pc;
    nnValue v;

    if (o->sig != SIG_OBJECT) fatal("SIG_OBJECTL at prepare");

    for (d=0;d<S->n_chains;d++) {
        pc = S->chains[d];
        for (i=0; i<pc->k; i++) {
            pc->lf->get_value(o,i+1,&v);
            o->values[pc->offset + i] = v.i;
        }
        o->values[pc->offset + i] = ASTERISK_VALUE;
    }

    o->ok = 1;
}



/*--------------------------
*
*
*
*/
int nn_has_single_child(nnPNode node) {
    if (node->branches == NULL) return 0;
    return node->branches->next == NULL;
}

/*--------------------------
*
*
*
*/
nnPNode nn_find_child_by_ivalue(nnPNode node, int ival, nnPBranch *vc_out) {
    nnPBranch vc;
    if (!vc_out) fatal("VC_OUT 01");
    SIG_TEST(NODE,node);
    // varre todos os valores do node
    for (vc = node->branches; vc; vc = vc->next) {
        if (ival == vc->value.i)  {
            if (vc_out) *vc_out = vc;
            return vc->child;
        }
    }
    return NULL;
}

#define __ORDERED

/*--------------------------
*
*
*
*/
nnPBranch nn_add_branch(nnPBranch *nodes, nnPNode child, nnPValue v,int shared_flag) {
    nnPBranch vc = nn_create_value_child();

    // inicializa child
    memcpy(&vc->value,v,sizeof(nnValue));
    vc->child = child;
    vc->shared_child = shared_flag;

#ifdef __ORDERED__
    {
        nnPBranch p,prev;

        vc->next = NULL;
        for (p = *nodes, prev = NULL; p; prev = p, p = p->next);
        if (!prev) {
            *nodes = vc;
        } else {
            prev->next = vc;
        }
    }
#else
    // insere o novo child na lista
    vc->next = *nodes;
    *nodes = vc;
#endif
    return vc;
}

#define PROPER_CHILD 0
#define SHARED_CHILD 1

/*--------------------------
*
*
*
*/
nnPNode nn_new_proper_child(nnPNode node, nnPValue v) {
    nnPNode child = nn_create_node();
    if (node->sig != SIG_NODE) fatal ("SIG_NODE 663");
    nn_add_branch(&node->branches,child,v,PROPER_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_proper_child_ex(nnPNode node, nnPValue v, nnPBranch *vc) {
    nnPNode child = nn_create_node();
    if (node->sig != SIG_NODE) fatal ("SIG_NODE 633");
    *vc = nn_add_branch(&node->branches,child,v,PROPER_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_proper_child_ival(nnPNode node, int ival) {
    nnValue v;
    nnPNode child = nn_create_node();

    v.i = ival;
    nn_add_branch(&node->branches,child,&v,PROPER_CHILD);
    return child;
}


/*--------------------------
*
*
*
*/
nnPNode nn_new_shared_child(nnPNode node, nnPValue v,nnPNode child) {
    if (node->sig != SIG_NODE) fatal ("SIG_NODE 733");
    nn_add_branch(&node->branches,child,v,SHARED_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_shared_child_ival(nnPNode node, int ival,nnPNode child) {
    nnValue v;
    v.i = ival;
    nn_add_branch(&node->branches,child,&v,SHARED_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
nnPNode nn_new_shared_child_ex(nnPNode node, nnPValue v,nnPNode child, nnPBranch *vc) {
    *vc = nn_add_branch(&node->branches,child,v,SHARED_CHILD);
    return child;
}

/*--------------------------
*
*
*
*/
void nn_set_shared_content(nnPNode receiver, nnPNode source) {
    // $NOVO
#ifdef __NOVO__
    receiver->content_type = SHARED_CONTENT;
#endif
    receiver->shared_content = 1;
    receiver->terminal = source->terminal;
    if (source->terminal) {
        SIG_TEST(TERMINAL,source->c_t_n.terminal);
        receiver->c_t_n.terminal = source->c_t_n.terminal;
    } else {
        SIG_TEST(NODE,source->c_t_n.content);
        receiver->c_t_n.content = source->c_t_n.content;
    }
}

#ifndef __NOVO__
/*--------------------------
*
*
*
*/
void nn_set_proper_content(nnPNode receiver, nnPNode node) {
    receiver->shared_content = 0;
    receiver->terminal = 0;
    receiver->c_t_n.content = node;
}
#endif

/******************************************************************************************//*

    Time Series Functions para o algoritmo

*//******************************************************************************************/

#ifdef __NOVO__
/*--------------------------
*
*
*
*/
void nn_set_proper_terminal_ex(nnPNode receiver, nnPTerminal pt,int content) {
    receiver->shared_content = 0;
    if (!IS_OMEGA(content)) fatal("Not Omega");
    receiver->content_type = content;
    receiver->terminal = 1;
    SIG_TEST(TERMINAL,pt);
    receiver->c_t_n.terminal = pt;
}
#else
/*--------------------------
*
*
*
*/
void nn_set_proper_time_series(nnPNode receiver, nnPTerminal pt) {
    receiver->shared_content = 0;
    receiver->terminal = 1;
    receiver->c_t_n.terminal = pt;
}
#endif


/******************************************************************************************//*

    Function Shallow Copy

*//******************************************************************************************/

/*--------------------------
*
*
*
*/
nnPNode nn_shallow_copy(nnPNode node) {
    nnPNode new_node;
    nnPBranch vc;

    new_node = nn_create_node();

    // set shared content
    nn_set_shared_content(new_node,node);

    // passa por todos os value-childs do node
    for (vc = node->branches; vc; vc = vc->next) {
        // adiciona em new_node novos registros do tipo value_child
        // onde o campo child é shared (vc->child)
        nn_new_shared_child(new_node, &vc->value, vc->child);
    }
    return new_node;
}



#ifdef __NOVO__

/******************************************************************************************//*

    $NOVO $NOVO $NOVO
    Alteracoes para poder fazer a contagem dos ramos

*//******************************************************************************************/

/*--------------------------
*
*
*
*/
nnPNode nn_set_proper_content_ex(nnPNode receiver, nnPNode node,int mode) {
    receiver->shared_content = 0;
    receiver->terminal = 0;
    receiver->content_type = mode;
    receiver->c_t_n.content = node;
    return receiver;
}

/*--------------------------
*
*  @@@@ Como esta função não foi descrita no paper, tive de advinhar o que fazia
*
*/
nnPNode nn_replace_child_ex(nnPBranch vc) {
    nnPNode child;

    //if (HAS_NO_CHILD(vc->child)) {
    //    aprintf("REPLACE CHILD - shared child HAS NO child. %p\n",vc->child->c_t_n.content);
    //    node = nn_create_node();
    //    if (vc->child->terminal) {
    //    } else {
    //        nn_set_shared_content(node,vc->child);
    //    }

    //    vc->child = node;
    //} else {
    //    aprintf("REPLACE CHILD - shared child HAS children.\n");
    //    vc->child = nn_shallow_copy(vc->child);;
    //}


    child = nn_shallow_copy(vc->child);
    vc->child = child;
    vc->shared_child = 0;

    return vc->child;
}


/*--------------------------
*
*
*
*/
void check_terminal_node(nnPNode node,nnPSchema S, int d,int level) {
    nnPBranch e;
    return;

    if (!node->branches) {
        // desce um nivel
        if (d != S->n_chains) {
            check_terminal_node(node->c_t_n.content,S,d+1,level+1);
            if (node->terminal) fatal("Should not be Terminal");
        } else {
            if (!node->terminal) fatal("Should be Terminal");
            SIG_TEST(TERMINAL,node->c_t_n.terminal);
            if (node->shared_content && (!node->branches)) fatal("Shared Content at lower level %d",level);
            // printf("bottom %d %d\n",d,level);
            return;
        }
    } else {
        if (d == S->n_chains) {
            if (node->shared_content && (level==S->n_chains+S->n_values)) fatal("Shared Content at lower level %d!",level);
        }

        for (e = node->branches; e; e=e->next) {
            check_terminal_node(e->child,S,d,level+1);
        }
    }
}

/*--------------------------
*
*
*
*/
int check_sums(nnPNode node,nnPSchema S, int d,int level,nnPObject obj) {
    nnPBranch e;
    //return 0;
    SIG_TEST(NODE,node);

    if (!node) return 0;

    if (!node->branches) {
        // desce um nivel
        if (d != S->n_chains) {
            if (node->terminal) fatal("SUM: Should not be Terminal dim:%d level:%d",d,level);
            if (node->terminal)
                return S->pit->count(node->c_t_n.terminal);
            else {
                if (!node->c_t_n.content) return 0;
                return (check_sums(node->c_t_n.content,S,d+1,d+S->chains[d]->offset,obj));
            }
        } else {
            if (!node->terminal)
                fatal("SUM: Should be Terminal");
            SIG_TEST(TERMINAL,node->c_t_n.terminal);
            if (node->shared_content && (!node->branches)) fatal("SUM: Shared Content at [%p] dim %d level %d!!",node,d,level);
            // printf("bottom %d %d\n",d,level);
            return S->pit->count(node->c_t_n.terminal);
        }
    } else {
        int sumc, sumbeta;
        if (d == S->n_chains) {
            if (node->shared_content && (level==S->n_chains+S->n_values))
                fatal("SUM: Shared Content at level %d!!!",level);
        }

        sumc = 0;
        for (e = node->branches; e; e=e->next) {
            if (e->child->sig == SIG_NODE)
                sumc += check_sums(e->child,S,d,level+1,obj);
        }
        if (node->terminal)
            sumbeta = S->pit->count(node->c_t_n.terminal);
        else
            sumbeta = check_sums(node->c_t_n.content,S,d+1,d+S->chains[d]->offset,obj);

        if (sumc != sumbeta) {
            printf("Somas [%p]:\n",node);
            for (e = node->branches; e; e=e->next) {
                nnPNode child = e->child;
                if (e->child->sig == SIG_NODE) {

                    if (!e->shared_child) {
                        printf("child[%p] Shared(N) (%d) sum:%d\n",child,e->value.i,check_sums(e->child,S,d,level+1,obj));
                    } else {
                        printf("child[%p] Shared(S) (%d)  sum:%d\n",child,e->value.i,check_sums(e->child,S,d,level+1,obj));
                        if (child->terminal) {
                            printf("    Terminal [%p] count: %d\n",child->c_t_n.terminal,terminal_count(S->pit,child->c_t_n.terminal));
                        }
                    }
                }
            }
            printf("Content: [%p] (%d) sum:%d\n",node->c_t_n.content,obj ? obj->values[level-1] : -1, sumbeta);
            fatal("Somas inconsistentes: content=%d children=%d node[%p] level:%d",sumbeta, sumc,node,level);
        }
        return sumc;
    }
}

/*--------------------------
*
*
*
*/
void check_ref_purge(nnPNode node,nnPSchema S, int d,int level,nnPNodes betas) {
    nnPBranch e;
    nnPNode content ;

    if (!node->branches) {
        // desce um nivel
        if (d != S->n_chains) {
            if (node->terminal) fatal("Should not be Terminal");
            content = node->c_t_n.content;
            if (nn_node_in_nodes(betas,content)) fatal("Invalid Reference");
            check_ref_purge(node->c_t_n.content,S,d+1,level+1,betas);
        } else {
            if (node->shared_content) fatal("Shared Content at lower level");
            if (!node->terminal) fatal("Should be Terminal");
            SIG_TEST(TERMINAL,node->c_t_n.terminal);
            // printf("bottom %d %d\n",d,level);
            return;
        }
    } else {
        if (d == S->n_chains) {
            if (node->shared_content && (level==S->n_chains+S->n_values)) fatal("Shared Content at lower level %d",level);
        } else {
            content = node->c_t_n.content;
            if (nn_node_in_nodes(betas,content)) fatal("Invalid Reference");
        }

        for (e = node->branches; e; e=e->next) {
            check_ref_purge(e->child,S,d,level+1,betas);
        }
    }
}

/*--------------------------
*
*
*
*/
void check_object_path(nnPNode root, nnPChain chain, nnPObject obj, int dimension, int principal) {
    int i;
    nnPNode child;
    nnPBranch vc;
    nnPNode node;
    int level;
    int ival;


    if (!root) fatal("root");
    node = root;

    level = chain->offset + dimension - 1; // reserva espaco para o child

    for (i = 0; i < chain->k; i++) {
        if (node->shared_content) {
            if (!root->branches) fatal("alpha node with shared content");
            if (root->branches->next) fatal("node with 2 or more childs and shared content");
        } else {

        }

        ival = obj->values[chain->offset+i];
        child = nn_find_child_by_ivalue(node,ival,&vc);

        // a: Nao encontrou uma aresta com o valor desejado
        if (!child) fatal("object not found");

        if (vc->shared_child) {
            break;
        }

        child = node;
    }
}

/******************************************************************************************//*

    Function TrailProperPath_ex

*//******************************************************************************************/



/*--------------------------
*
*
*
*/
nnPStack nn_trail_proper_path_ex(nnPNode root, nnPChain chain, nnPObject obj, int dimension, nnPXtree xtree) {
    int i;
    nnPStack stack;
    nnPNode child;
    nnPBranch vc;
    nnPNode node;
    int level;
    int ival;

    stack = nn_create_stack();
    nn_push(stack,root);

    if (xtree->done) return stack;

    node = root;
    level = chain->offset + dimension - 1; // reserva espaco para o child

    for (i = 0; i < chain->k; i++) {
        xtree->path[level] = node;

        ival = obj->values[chain->offset+i];
        child = nn_find_child_by_ivalue(node,ival,&vc);

        // a: Nao encontrou uma aresta com o valor desejado
        if (!child) {

            // a1: a Xtree esta num caminho Proper ou e a Xtree principal?
            if ((xtree->op == OP_FORK) || (xtree->src == NULL)) {

                // cria o novo node
                child = nn_new_proper_child_ival(node,ival);
                if (xtree->op == OP_NONE) { xtree->op = OP_FORK; }

            // a2: a Xtree origem tem modificacoes?
            } else if (xtree->src->op != OP_NONE) {
                child = xtree->src->path[level+1];                 // usa o child visitado
                if (!child) fatal("a2: child offset:%d level:%d",chain->offset,level+1);
                nn_new_shared_child_ival(node, ival, child);       // cria um Shared
                xtree->path[level+1] = child;                      // usa o child visitado

                xtree->done = 1;                                   // nao analisa mais nenhuma Ctree
                nn_xtree_inherits_rest(xtree,level);               // herda o restante do cominho
                if (xtree->op == OP_NONE) { xtree->op = OP_SHARE; }
                Iprintf("root[%p] Share child [%p] -> [%p]. OP=%d\n", xtree->root, node, child,xtree->op);

                break;

            // a3: child==NULL e Xtree origem sem modificacao
            } else {
                fatal("Erro: child==NULL e Xtree origem sem modificacao");
            }

        // b: encontrou a aresta com valor v e ela e Shared?
        } else if (vc->shared_child) {

            // b0: Esta na Xtree principal
            if (!xtree->src) {
                fatal("Erro: Shared Child in Xtree principal");

            // b1: A Xtree origem não foi modificada?
            } else if (xtree->src->op == OP_NONE) {
                nn_xtree_inherits_rest(xtree,level);
                //xtree->done = 1;
                break;

            // b2: o child encontrado está no caminho percorrido?
            } else if (child == xtree->src->path[level+1]) {
                nn_xtree_inherits_rest(xtree,level);
                xtree->done = 1;
                break;

            // b3: o child encontrado não está no caminho
            } else {
                nnPNode prevChild = vc->child;
                child = nn_replace_child_ex(vc);

                // use este child para os novos links relativos
                xtree->src->path[level+1] = child;

                if (xtree->op == OP_NONE) { xtree->op = OP_DESHARE; }
                Iprintf("root[%p] Replace child node [%p] [%p] => [%p]. OP=%d  Shared=%d\n",xtree->root,node,prevChild,child,xtree->op,vc->shared_child);
            }

        // c: encontrou a aresta com valor ival e ela não era Shared
        } else {
            // continua o caminho
        }

        nn_push(stack,child);
        node = child;
        level = level + 1;
    }
    // coloca a ultima child no c
    xtree->path[level] = node;
    return stack;
}


/******************************************************************************************//*

    function nn_add_ex

*//******************************************************************************************/


/*--------------------------
*
*
*
*/
void nn_add_ex(nnPNode root, nnPObject o, int dimension, nnPSchema S, nnPNodes updated_nodes, nnPXtree xtree) {
    nnPStack stack;
    nnPNode node, child;
    int update;

    // $NOVO
    nnXtree new_xtree;
    nnPXtree param_tree;

    stack = nn_trail_proper_path_ex(root, S->chains[dimension-1], o, dimension, xtree);
    child = NULL;

    xprintf("[%d] root[%p] OP = %d\n",dimension,root,xtree->op);

    while (! nn_stack_is_empty(stack)) {

        node = nn_pop(stack);
        update = 0;

        //
        if (nn_has_single_child(node)) {
            // como a stack pode ter apenas a raiz,
            //    pode não haver child disponivel
            if (!child) { child = node->branches->child; }
            nn_set_shared_content(node,child);
            //printf("1 %p\n",node);
        //
        } else if (node->c_t_n.content == NULL) {
            xprintf("2\n");
            if (node->shared_content || node->terminal)
                fatal("**** Abnormal strucuture ****\n");

            if (dimension == S->n_chains) {
                update = HAS_NO_CHILD(node) ? OMEGA_CONTENT : BETA_OMEGA_CONTENT;
                nn_set_proper_terminal_ex(node,terminal_create(S->pit),update);
                if (IS_BETA(node->content_type)) fatal("Wrong Omega Beta");
                //printf("2a %p\n",node);

            } else {
                // indica que a arvore que sera atualizada é uma alpha
                update = ALPHA_CONTENT;
                nn_set_proper_content_ex(node, nn_create_node(),ALPHA_CONTENT);
                //printf("2b %p\n",node);
            }
            nn_stats.n_alphas++;

        // paper: if contentIsShared(node) and content(node) not in updated_nodes
        // o node esta marcado como shared mas tem mais de um filho!!
        } else if (node->shared_content) {
            xprintf("3a\n");

            // time series?
            if (dimension == S->n_chains) {
                //if (!nn_node_in_nodes(updated_nodes,(nnPNode) node->c_t_n.terminal)) {
                    nnPTerminal old,novo;
                    xprintf("3b\n");
                    if (!node->terminal) fatal("Should be terminal! %p -> %p",node,node->c_t_n.terminal);
                    SIG_TEST(TERMINAL,node->c_t_n.terminal);
                    novo = terminal_clone(S->pit, node->c_t_n.terminal);

                    old = node->c_t_n.terminal;
                    xprintf("[%d] root[%p] [%p] clonado para [%p] => %d\n",dimension,root,old,novo,terminal_count(S->pit,novo));
                    update = HAS_NO_CHILD(node) ? OMEGA_CONTENT : BETA_OMEGA_CONTENT;
                    nn_set_proper_terminal_ex(node, novo, update);
                    if (node->shared_content) fatal("Shared");
                    //printf("3a %p\n",node);

                    if (IS_BETA(update)) nn_stats.n_betas++; else nn_stats.n_alphas++;

                    // nao incrementa duas vezes
                    if (nn_node_in_nodes(updated_nodes,(nnPNode) old)) {
                        xprintf("found\n");
                        update = 0;
                    } else {
                        xprintf("Not found in nodes: %p\n",node->c_t_n.content);
                    }
                //}
            // usual content
            } else if (!nn_node_in_nodes(updated_nodes,node->c_t_n.content)) {

                nnPNode newnode,child;
                xprintf("3c\n");
                if (node->terminal) fatal("terminal");
                update = HAS_NO_CHILD(node) ? ALPHA_CONTENT : BETA_CONTENT;
                newnode = nn_shallow_copy(node->c_t_n.content);
                child = newnode->branches?newnode->branches->child:NULL;
                Iprintf("Shared to Proper Content [%p] => [%p] -> [%p] . Update: %d Copy proper content\n",node,newnode,child,update);
                nn_set_proper_content_ex(node, newnode, update);
                if (IS_BETA(update)) nn_stats.n_betas++;  else nn_stats.n_alphas++;
            } else {
                xprintf("3d\n");
            }
           // Original: update = 1;

        } else if (!node->shared_content && node->c_t_n.content) {
            xprintf("4\n[%d] root[%p]   node[%p]\n",dimension,root,node);
            trace1("Not Shared Content. Just update\n");

            // Original
            // update = 1;

            // $NOVO
            // indica que a arvore que sera atualizada seguira o content_mode do node
            update = node->content_type; // ALPHA_CONTENT ou BETA_CONTENT
            if (update == 0) fatal ("Erro!\n");
        }

        // $NOVO
        if (HAS_MULTI_CHILD(node) && (!IS_BETA(node->content_type) || node->shared_content)) {
            fatal("Inconsistencia 0");
        }

        if (update) {
            if (dimension == S->n_chains) {
                int c;
                if (!IS_OMEGA(update)) fatal("Not omega");
                //print_values(o,28);
                c = terminal_insert(S->pit,node->c_t_n.terminal,o);
                xprintf("Update[%d]: Inserting at [%p] => %d \n",dimension,node->c_t_n.terminal,c);
                if ((c >= 2) && (xtree->src == NULL)) nn_stats.n_iguais ++;
            } else {
                if (IS_ALPHA(update)) {
                    param_tree = xtree;

                } else if (IS_BETA(update)) {
                    param_tree = nn_xtree_prepare(&new_xtree, xtree, dimension+1,node->c_t_n.content);

                } else {
                    fatal("Abnormal execution: Not Alpha or Beta!!!");
                    param_tree = NULL;  // evitar warning
                }

                nn_add_ex(node->c_t_n.content, o, dimension+1, S, updated_nodes, param_tree);
            }

            // Ponto de INSERÇÃO no algoritmo do paper
            // segundo o paper teria aqui: nn_insert_in_nodes(updated_nodes,node->c_t_n.content);
            nn_insert_in_nodes(updated_nodes,node->c_t_n.content);
            xprintf("Inserted in nodes: %p\n",node->c_t_n.content);
            if (!nn_node_in_nodes(updated_nodes,node->c_t_n.content)) fatal("Missing node");
        }

        child = node;
    }

    nn_release_stack(stack);
    // trace2("end add: %d\n",dimension);
}

#endif


/*--------------------------
*
*
*
*/
void nn_do_purge(nnPNode node, nnPNode target, nnPNodesToFree rlist,PITerminal pit) {
    nnPBranch pb, next, dummy;
    nnPNode child;
    nnPNode target_child;

    Rprintf("Purging node[%p] target[%p]...\n",node,target);
    fflush(stdout);

    //printf("Purging %p\n",node);
    if (node->shared_content ^ (node->content_type == SHARED_CONTENT)) fatal("sdfjksdfklj");

    // purge todos os childs
    for (pb = node->branches; pb; pb = next) {
        //printf("1\n");    fflush(stdout);

        next = pb->next;
        child = pb->child;
        // SIG_TEST(NODE,child);

        if (!pb->shared_child && child->sig == SIG_NODE) {
            // localiza o node do target para onde os shareds de outras xtrees deve apontar
            target_child = nn_find_child_by_ivalue(target,pb->value.i,&dummy);
            if (!target_child) {
                // pode ter sido removido.
                Rprintf("Value %d NOT FOUND while purging node [%p]  target[%p] sig:%x\n",pb->value.i,node,target,target->sig);
                //fatal("Value %d not found. node [%p]  target[%p]",pb->value.i,node,target);
            } else {
                Rprintf("Value %d FOUND while purging node [%p]  target[%p]\n",pb->value.i,node,target);
            }

            // remove o child
            Rprintf("do_purge on proper child [%p]\n",child);
            if (child->sig == SIG_NODE_REMOVED) {
                Rprintf("Proper node was deleted!!!!\n");
            } else if (child->sig == SIG_NODE) {
                nn_do_purge(child, target_child, rlist, pit);
            }
        }
        Rprintf("Releasing edge...\n");
        nn_release_value_child(pb);
    }
    node->branches = NULL;
    //printf("2\n");    fflush(stdout);


    // Purge apenas Proper Content
    if (!node->shared_content) {
        //printf("3\n");    fflush(stdout);
        if (node->terminal) {
            nnPTerminal pt = node->c_t_n.terminal;
            if (pt->sig == SIG_TERMINAL) {
                SIG_TEST(TERMINAL,pt);
                Rprintf("$$$ do_purge: terminal_release [%p]\n",pt);
                terminal_release(pit,pt);
            } else fatal("No SIG_TERMINAL");
        } else {
            nnPNode content = node->c_t_n.content;
            if (content->sig == SIG_NODE) {
                Rprintf("$$$ do_purge: content_release [%p]\n",content);
                nn_do_purge(content, target->c_t_n.content, rlist, pit);
            }  else fatal("No SIG_NODE");
        }
        if (IS_BETA(node->content_type)) nn_stats.n_betas--; else nn_stats.n_alphas--;
        node->c_t_n.content = NULL;
    }

    //printf("8\n");    fflush(stdout);
    SIG_TEST(NODE,node);
    Rprintf("Purging node [%p]... ",node);
    nn_node_to_free(rlist,node,target);
    Rprintf("done\n");
}


/*--------------------------
*
*
*
*/
void nn_redirect_purged_nodes(nnPNode node) {
    nnPBranch pb;
    nnPNode child;

    if (node->sig == SIG_NODE_REMOVED) {
        Rprintf("//// Redirect return\n");
        return;
    }
    //fatal("removed");

    //printf("A1\n");    fflush(stdout);
    // relink todos os childs
    for (pb = node->branches; pb; pb = pb->next) {
        //printf("A2\n");    fflush(stdout);
        child = pb->child;
        //printf("A2 %p\n",child);    fflush(stdout);
        if (!child)
            fatal("Missing Child");

        if (!pb->shared_child) {
            //printf("A2a\n");    fflush(stdout);
            nn_redirect_purged_nodes(child);
        } else {
            //printf("A2b\n");    fflush(stdout);

            if (child->sig == SIG_NODE_REMOVED) {
                //printf("A2c\n");    fflush(stdout);
                if ( child->branches ) {
                    nnPNode new_child = (nnPNode) child->branches;
                    //printf("A2d\n");    fflush(stdout);
                    Rprintf("//// RELINK: node [%p]  value: %d [%p] => [%p]\n",node,pb->value.i,child,new_child);
                    pb->child = new_child;
                }
            }
            //printf("A2e\n");    fflush(stdout);
        }
    }

    //printf("A3\n");    fflush(stdout);
    // Relink apenas Proper Content
    if (!node->shared_content) {
        if (!node->terminal) {
            nn_redirect_purged_nodes(node->c_t_n.content);
        }
    }
       //printf("A4\n");    fflush(stdout);
}

/*--------------------------
*
*
*
*/
int nn_remove_edge_child(nnPNode node, nnPBranch edge) {
    nnPBranch vc, prev;

    // varre todos os filhos de node procurando child
    for (prev=NULL, vc=node->branches; vc; prev=vc, vc = vc->next) {

        if (vc != edge) continue;
        // xprintf("Removed Value Child %c\n",vc->value.i-10+65);

        // vc retira da lista
        if (prev) {
            prev->next = vc->next;
        } else {
            node->branches = vc->next;
        }
        vc->next = NULL;
        nn_release_value_child(vc);

        return 1;
    }

    fatal("ERRO SEVERO - 1229");
    return 0;
}


/*--------------------------
*
*  Retorna a raiz
*
*/
int nn_remove_step(nnPNode root,nnPSchema S, nnPObject obj,int dim,nnPNodesToFree rlist,nnPNodes betas) {
    nnPNode node = root;
    nnPNode child;
    nnPNode path[MAX_PATH];
    nnPBranch edge;
    nnPBranch edges[MAX_PATH];
    int level0 = S->chains[dim-1]->offset;
    int removeEdge = 0;
    int length = S->n_values + S->n_chains;
    int v;
    int level = level0;
    int purged = 0;
    int has_content;

    if (obj->sig != SIG_OBJECT) fatal("SIG_OBJECTL at release");

    Rprintf("******* Dimensao: %d lenght: %2d level: %2d  root:[%p]\n",dim,length,level,root);
    while (level < length) {
        path[level] = node;
        v = obj->values[level];

        //if (level >= length-5)
            Rprintf("Level: %2d  Node: [%p] Value:%2d\n",level,node,v);

        if (v == ASTERISK_VALUE) {
            //printf("level %d\n",level);
            if (node->shared_content) {
                if (node->branches) fatal("Shared Content and Children on path");
                if (!node->terminal) fatal("Terminal problem");
                //printf("level (shared) %d\n",level);
                break;
            }
            child = node->c_t_n.content;
            edges[level] = NULL;

        } else {
//            Rprintf("Finding Child: %d",v);
            child = nn_find_child_by_ivalue(node,v,&edge);
//            Rprintf(" Done\n");
            edges[level] = edge;
            if (!child) {
                //fatal("Value not found");
                Rprintf("********** Value not found!\n");
                return 0;
            } else if (edge->shared_child) {
                removeEdge = (child->sig == SIG_NODE_REMOVED && !child->branches);
                Rprintf(" BREAK ON Shared child [%p] -> [%p] %d. Remove Edge = %d\n",node,child,edge->value.i,removeEdge);
                break;
            }
        }
        node = child;
        level ++;
    }

    Rprintf("End Level: %d           Start Level:%d\n",level,level0);
    if (level >= length) {
        int c;
        nnPTerminal pt = (nnPTerminal) node;
        if (pt->sig != SIG_TERMINAL) {
            Rprintf("$$$ count: terminal_count [%p]\n",pt);
            removeEdge = 0;
        } else {
            c = terminal_count(S->pit,pt);
            Rprintf("Count before remove %d\n",c);
            terminal_remove(S->pit,pt,obj);
            Rprintf("Remove from terminal [%p]\n",pt);
            c = S->pit->count(pt);
            Rprintf("Count level0 %d after %d\n",level0,c);
            removeEdge = c == 0;
            if (removeEdge) {
                // nn_stats.n_betas--;
                Rprintf("$$$ end_level: terminal_release [%p]\n",pt);
                Rprintf("@@@@ Removing alpha terminal from xtree %s %p ...  ",level0==0?"principal":"derivada",pt);
                terminal_release(S->pit,pt);
                Rprintf("Done\n");
            }
        }
        level --;
    }

    Rprintf("delete loop until level %d\n",level0);
    //

    while (level >= level0) {
        node = path[level];
        edge = edges[level];
        v  = obj->values[level];
        has_content = 0;

        SIG_TEST(NODE,node);

        Rprintf("Remove? %d   Remove level: %d value:%d node:[%p]  child:[%p]\n",removeEdge, level, v, node,edge?edge->child:NULL);
        if (!removeEdge) {
            if (!node->branches) {

            } else if (HAS_SINGLE_CHILD(node)) {
                nnPNode child = node->branches->child;

                    if (!node->shared_content)
                        fatal("single Node without Shared content");

                    if (node->branches->shared_child) {
                        // Rprintf("SINGLE CHILD IS SHARED\n");
                        if (node->sig == SIG_NODE_REMOVED) {
                            fatal("SINGLE CHILD IS SHARED AND RELEASED\n");
                        }
                    }

                    SIG_TEST(NODE,child);
                    if (node->content_type != SHARED_CONTENT)
                        fatal("MISSED SHARED_CONTENT %d",node->content_type);

                    if (child->terminal && child->shared_content)
                        fatal("Child content is terminal and shared content");

                    if (child->c_t_n.content != node->c_t_n.content) {
                        Rprintf("Updating shared content for node [%p]\n",node);
                        nn_set_shared_content(node,child);
                    }

            } else if (node->shared_content) {

            //                 node->content_type == BETA_CONTENT
            } else if ((v != ASTERISK_VALUE) && (IS_BETA(node->content_type)) ) {
                if (node->terminal)  {
                    Rprintf("Remove from beta terminal [%p]\n",node->c_t_n.terminal);
                    terminal_remove(S->pit,node->c_t_n.terminal,obj);
                } else {
                    Rprintf("<<<<< analizing beta ...\n");
                    nn_remove_step(node->c_t_n.content,S,obj,dim+1,rlist,betas);
                    has_content = 1;
                    Rprintf(">>>>> Done analizing beta\n");
                }
            }
        } else {
            if (v == ASTERISK_VALUE) {
                if (edges[level-1]->shared_child) fatal("Primary alpha is shared");
                Rprintf("Alpha: Releasing node [%p]... ",node);
                nn_node_to_free(rlist,node,NULL);
                    Rprintf("done\n");
                nn_stats.n_alphas--;

            } else {
                removeEdge = 0;
                Rprintf("Removing edge... ");
                nn_remove_edge_child(node,edge);
                Rprintf("done\n");

                // ficou sem filhos
                if (!node->branches) {
                    if (!node->shared_content)
                        fatal("Single Node with proper content");

                    // nao remove a raiz
                    if (level==level0) {
                        node->terminal = 0;
                        node->c_t_n.content = NULL;
                        node->content_type = SHARED_CONTENT;

                    } else {
                        Rprintf("No Child: Releasing node [%p]... ",node);
                        //nn_insert_in_nodes(rlist,node);
                        SIG_TEST(NODE,node);
                        nn_node_to_free(rlist,node,NULL);
                        Rprintf("done\n");
                    }
                    removeEdge = 1;

                // tornou-se filho unico
                } else if (nn_has_single_child(node)) {
                    nnPNode child = node->branches->child;
                    int terminal = node->terminal;

                    if (!child) fatal("Missing Child\n");
                    if (terminal != child->terminal) fatal("Child content terminal problem");

                    if (node->shared_content)
                        fatal("Non single Node with Shared content");

                    if (node->branches->shared_child) {
                        Rprintf("SINGLE CHILD IS SHARED\n");
                        if (node->sig == SIG_NODE_REMOVED) {
                            fatal("SINGLE CHILD IS SHARED AND RELEASED\n");
                        }
                    }

                    SIG_TEST(NODE,child);
                    if (!IS_BETA(node->content_type))
                        fatal("MISSED BETA_CONTENT %d",node->content_type);

                    nn_stats.n_betas--;
                    if (terminal) {
                        nnPTerminal pt = node->c_t_n.terminal;
                        //fatal("TERMINAL X");
                        SIG_TEST(TERMINAL,pt);
                        Rprintf("$$$ single_child: terminal_release [%p]\n",pt);
                        if (node->c_t_n.terminal->sig == SIG_TERMINAL) {
                            terminal_release(S->pit,pt);
                        } else {
                            fatal("Invalid SIG_NODE");
                        }
                        SIG_TEST(TERMINAL,child->c_t_n.terminal);
                        Rprintf("$$$ single_child: child's content [%p]\n",child->c_t_n.content);

                    } else {
                        Rprintf("Remove As Purge... ");
                        if (nn_remove_step(node->c_t_n.content,S,obj,dim+1,rlist,betas))
                           fatal("Empty Beta");
                        Rprintf("done\n");
                        //Rprintf("do_purge [%p]\n",node->c_t_n.content);
                        //nn_insert_in_nodes(betas,node->c_t_n.content);
                        nn_do_purge(node->c_t_n.content, child->c_t_n.content, rlist,S->pit);
                        purged = 1;
                    }

                    SIG_TEST(NODE,child);
                    Rprintf("Set Shared Content [%p] [%p/%p] terminal? %d... ",node,child,child->c_t_n.content,child->terminal);
                    nn_set_shared_content(node,child);
                    if (terminal != child->terminal) fatal("Child content terminal problem 2");

                    Rprintf("done\n");

                // mais de um filho
                } else {
                    if (IS_BETA(node->content_type)) {
                        if (node->shared_content) fatal("Inconsistencia: Shared");
                        if (node->terminal) {
                            int c;
                            nnPTerminal pt = node->c_t_n.terminal;
                            SIG_TEST(TERMINAL,pt);
                            Rprintf("DECREMENT TERMINAL ... ");
                            S->pit->remove(pt,obj);
                            c = S->pit->count(pt);
                            if (c == 0) {
                                fatal("Terminal Zero");
                            }

                            //printf("REMOVE BETA TERMINAL ... ");
                            //if (pt->sig == SIG_TERMINAL)
                            //    nn_release_time_series(pt);
                            // printf("Time Series: %x  %d\n",pt->sig,pt->n);
                            //printf("done\n");
                        } else {
                            Rprintf("REMOVE STEP... ");
                            if (nn_remove_step(node->c_t_n.content,S,obj,dim+1,rlist,betas))
                                fatal("Empty Beta");
                            has_content = 1;

                            Rprintf("done\n");
                        }
                    }
                }
            }
        }

        if (purged && has_content) {
            if (!node->terminal && !node->shared_content) {
                Rprintf("Redirect removed edges\n");
                nn_redirect_purged_nodes(node->c_t_n.content);
            }
        }



        level --;
    }

    Rprintf("Exiting removeEdge: %d ...\n",removeEdge);
    if (removeEdge) {
        Rprintf("[Remove Root]");
        node->c_t_n.content = NULL;
        SIG_TEST(NODE,node);
    }
    return removeEdge;
}

/*--------------------------
*
*
*
*/
nnPNode nn_remove_from_nanocubes(nnPNode root,nnPSchema S,nnPObject obj) {
    nnNodesToFree rlist;
    static int c = 0;
    int remove_root;
    nnPNodes betas;

    rlist.first = NULL;

    if (!obj->ok)
        nn_prepare_object_values(obj,S);

    Rprintf("--------------------------------------------\n");
    Rprintf("Removing: ");
    //print_values(obj,28);

    betas = nn_create_nodes();
    remove_root = nn_remove_step(root,S,obj,1,&rlist,betas);
    // check_ref_purge(root,S,1,0,betas);
    {
        nnPNodeItem p, pn;
        for (p=betas->first; p; p=pn) {
            pn = p->next;
            //nn_do_purge(p->node,&rlist,S->pit);
            free(p);
        }
        free(betas);
    }

    if(remove_root) {
        Rprintf("=========== REMOVE ROOT ==============\n");
    }
    nn_release_nodes_to_free(&rlist);

    c++;
    //printf("Remocoes: %d\n",c);
    return root;
}



#if 0
/*--------------------------
*
*
*
*/
nnPNode nn_del_from_nano_cube(nnPNode root,nnPSchema S,int dim, nnPObject obj) {
    nnPDeleted deleted;

    if (!obj->ok)
        nn_prepare_object_values(obj,S);

    deleted = nn_create_nodes();
    root = nn_del(root,S,dim,obj,deleted);
    nn_release_nodes(deleted,1);

    return root;
}
#endif

#endif


























/******************************************************************************************//*

    procedure nn_nano_cube

*//******************************************************************************************/

#ifndef __NOVO__
/*--------------------------
*
*
*
*/
void nn_add_to_nano_cube(nnPNode nano_cube, nnPSchema S, nnPObject o) {
    nnPNodes updated_nodes;

    updated_nodes = nn_create_nodes();
    nn_add(nano_cube,o,1,S,updated_nodes);
    nn_release_nodes(updated_nodes,0);
}


#else

// $NOVO
/*--------------------------
*
*
*
*/
void nn_add_to_nano_cube_ex(nnPNode nano_cube, nnPSchema S, nnPObject o) {
    nnPNodes updated_nodes;
    nnXtree xtree;

    if(!nano_cube) return;
    if (!o->ok) {
        nn_prepare_object_values(o,S);
        //printf("            ----- \n");
        //print_values(o,28);
    }

    updated_nodes = nn_create_nodes();

    nn_xtree_prepare(&xtree,NULL,1,nano_cube);

    nn_add_ex(nano_cube,o,1,S,updated_nodes,&xtree);

    nn_release_nodes(updated_nodes,0);
}
#endif

/*--------------------------
*
*
*
*/
nnPNode nn_nano_cube(nnPClass c, nnPSchema S) {
    nnPObject o;
#ifndef __NOVO__
    nnPNodes updated_nodes;
#endif
    nnPNode nano_cube = nn_create_node();

    // varre todos os objetos da classe
    for (o = c->first; o; o=o->next) {
        trace1("-----------------------\n");

#ifdef __NOVO__
        nn_add_to_nano_cube_ex(nano_cube,S,o);
#else
        updated_nodes = nn_create_nodes();
        nn_add(nano_cube,o,1,S,updated_nodes);
        nn_release_nodes(updated_nodes,0);
#endif
    }

    return nano_cube;
}



/******************************************************************************************//*

  TOP K

*//******************************************************************************************/

typedef struct tk_bucket {
    struct tk_bucket *prev, *next;
    struct tk_monitored *first;
    int64  counter;
} tkBucket, tkPBucket;

typedef union {
  int64 i64;
} tkID;

typedef struct tk_monitored {
    struct tk_bucket    *parent;
    struct tk_monitored *next;
    tkID   id;
    int64  epsilon;
} tkMonitored, tkPMonitored;




/******************************************************************************************//*

                  Quad-Tree virtual

  Implementa uma forma compacta de registrar uma quad-tree para fins do algoritmo

  Converte GMS (Graus, Minutos, Segungos) para uma representação binaria onde cada bit
  da esquerda para direita (MSB->LSB) representa uma decisão binária:

  O valor é menor ou igual (0) ao valor médio do intervalo ou é maior (1)?
  A cada decisão o intervalo escolhido é dividido na metade e o processo é repetido.

  O algoritmo primeiro converte de GMS para centésimos de segundos (precisão aprox. de 30 cm) e
  aplica divisoes sucessivas, o que gera 27 decisões até chegar ao precisão pretendida.

  ***** Isso não afeta o número de niveis que o algoritmo do nanocube efetivamente irá usar.
  Apenas oferece uma resolução maxima.

  Note que 27 decisões permitem armazenar todas as decisões de latitude e de longitude em 54 bitas,
  o que cabe com tranquilidade em um inteiro de 64 bits.
  Se for necessário pode-se reduzir as decisões para 26 para latitude e longitude caberem em 52 bits
  e poderem ser armazenados na mantissa de um double IEEE-754.

  O paragrafo abaixo foi REVOGADO: A latitude está variando de 0 a 180 !!
  Como as latitude variam apenas entre 0 e 180 (-90 e +90), decidi nesta versão considerá-las
  variando de 0 a 360 para que a resolução fosse equivalente a da longitude. Assim, a primeira
  decisao da latitude sempre será 0 (0..180). Isso é facilmente alterável se necessário.

  A conversão dispõe as decisões de latitude nos bits pares e as de longitude nos bits impares
  para que se possa selecionar uma parte hierarquica das coordenadas usando apenas uma única
  mascara de bits.

  Nivel    01 02 03 04 05 06 07 ... 27
  Bits     01 10 11 01 11 01 10 ... 10
  Lat      0_ 1_ 1_ 0_ 1_ 0_ 1_ ... 1_
  Lon      _1 _0 _1 _1 _1 _1 _0 ... _0


*//******************************************************************************************/

#define BITS_COORD 25
#define MAX_LATITUDE 85.05
#define BIT_RESOLUTION 100

#define M_PI 3.141592634

#include <math.h>

int long2tilex(double lon, int z)
{
	return (int)(floor((lon + 180.0) / 360.0 * pow(2.0, z)));
}

int lat2tiley(double lat, int z)
{
	return (int)(floor((1.0 - log( tan(lat * M_PI/180.0) + 1.0 / cos(lat * M_PI/180.0)) / M_PI) / 2.0 * pow(2.0, z)));
}

double tilex2long(int x, int z)
{
	return x / pow(2.0, z) * 360.0 - 180;
}

double tiley2lat(int y, int z)
{
	double n = M_PI - 2.0 * M_PI * y / pow(2.0, z);
	return 180.0 / M_PI * atan(0.5 * (exp(n) - exp(-n)));
}

/*--------------------------
*
*
*
*/
#if 1
uint64 gms_to_int64_base(double g,double m, double s,int lon,int quad) {
    uint64 shift;
    uint64 mask;
    uint64 result;
    uint64 seconds;
    uint64 inicio, fim, meio;
    int i, v;
    double extra;

    inicio = 0;
    // usa centesimo de grau.
    // É um  preciosismo para postergar erros de arredondamentos
    shift = quad ? 2 : 1;
    if (lon) {
        g += 180;
        fim = 360 * 3600 * BIT_RESOLUTION;
        mask = quad? 2: 1;
    } else {
        g = MAX_LATITUDE - g;
        fim = (int) ((2*MAX_LATITUDE) * 3600 * BIT_RESOLUTION);
        mask = 1;
    }

    extra = 0; // DESLOCAMENTO / 30.87;
    seconds = (uint64) ((g * 3600 + m * 60 + s + extra) * 100); // trunca para 1 casa decimal
    result = 0;
    i = 0;
    while (inicio < fim  && i < BITS_COORD) {
        meio = (fim + inicio) / 2;
        //printf("%02d, %I64d,  %I64d,  %I64d,  %I64d",i, seconds, inicio, meio, fim);
        result <<= shift;
        if (seconds <= meio) {
            fim = meio;
            v = 0;
        } else {
            inicio = meio + 1;
            result |= mask;
            v = 1;
        }

        //printf(",  %d\n",v);
        i ++;
    }
    while (i<BITS_COORD) {
        result <<= shift;
        i++;
    }
    return result;
}


/*--------------------------
*
*
*
*/
uint64 gms_to_int64(double g,double m, double s,int lon,uint64 acc) {
    return gms_to_int64_base(g,m,s,lon,1) | acc;
}


/*--------------------------
*
*
*
*/
uint64 gms_to_int64_prev(double g,double m, double s,int lon,uint64 acc) {
    uint64 mask;
    uint64 result;
    uint64 seconds;
    uint64 inicio, fim, meio;
    int i, v;
    double extra;

    inicio = 0;
    // usa centesimo de grau.
    // É um  preciosismo para postergar erros de arredondamentos
    if (lon) {
        g += 180;
        mask = 0x2; // bits impares
//        mask = 0x000200000000000000UL; // bits impares
        fim = 360 * 3600 * 100;
    } else {
        g += 90;
        g = 180 - g; // começa no sul
        mask = 0x1; // bits pares
//        mask = 0x0001000000000000UL; // bits pares
        fim = 180 * 3600 * 100;
    }

    extra = 0; // DESLOCAMENTO / 30.87;
    seconds = (uint64) ((g * 3600 + m * 60 + s + extra) * 100); // trunca para 1 casa decimal
    result = 0;
    i = 0;
    while (inicio < fim  && i < BITS_COORD) {
        meio = (fim + inicio) / 2;
        //printf("%02d, %I64d,  %I64d,  %I64d,  %I64d",i, seconds, inicio, meio, fim);
        result <<= 2;
        if (seconds <= meio) {
            fim = meio;
            v = 0;
        } else {
            inicio = meio + 1;
            result |= mask;
            v = 1;
        }

        //printf(",  %d\n",v);
        i ++;
    }
    while (i<BITS_COORD) {
        result <<= 2;
        i++;
    }
    return result | acc;
}
#else
uint64 gms_to_int64(double g,double m, double s,int lon,uint64 acc) {
    uint64 mask;
    uint64 result;
    uint64 seconds;
    uint64 inicio, fim, meio;
    int i, v;
    double extra;

    inicio = 0;
    // usa centesimo de grau.
    // É um  preciosismo para postergar erros de arredondamentos
    if (lon) {
        g += 180;
        mask = 0x8000000000000000UL; // bits impares
//        mask = 0x000200000000000000UL; // bits impares
        fim = 360 * 3600 * 100;
    } else {
        g += 90;
        g = 180 - g; // começa no sul
        mask = 0x4000000000000000UL; // bits pares
//        mask = 0x0001000000000000UL; // bits pares
        fim = 180 * 3600 * 100;
    }

    extra = 0; // DESLOCAMENTO / 30.87;
    seconds = (uint64) ((g * 3600 + m * 60 + s + extra) * 100); // trunca para 1 casa decimal
    result = acc;
    i = 1;
    while (inicio < fim  && i < 25) {
        meio = (fim + inicio) / 2;
        //printf("%02d, %I64d,  %I64d,  %I64d,  %I64d",i, seconds, inicio, meio, fim);
        if (seconds <= meio) {
            fim = meio;
            v = 0;
        } else {
            inicio = meio + 1;
            result |= mask;
            v = 1;
        }

        //printf(",  %d\n",v);
        mask >>= 2;
        i ++;
    }
    while (mask) {
        result <<= 2;
        mask >>= 2;
    }
    return result;
}
#endif

/*--------------------------
*
*
*
*/
void coord_to_gms(double coord,double *g,  double *m, double *s) {
    double dv;
    *g = (int) coord;
    dv = (coord - *g);
    if (dv<0) dv = -dv;
    *m = (int) (dv  * 60);
    *s = ((dv  * 3600) - (60 * *m));
}


/*=============================================================================*//*

        Teste Quad-Tree

*//*=============================================================================*/

/*--------------------------
*
*
*
*/
void toBin(int64 n,char *buf) {
    uint64 mask = 0x8000000000000000UL;
    int i = 0;
    while (mask > 0) {
        buf[i] = (n & mask)? '1' : '0';
        i++;
        mask >>= 1;
    }
    buf[i] = '\0';
}

/*--------------------------
*
*
*
*/
void test_gms_to_int(void) {
    char buf[66];
    int64 acc1, acc2;

    toBin(128,buf);
    printf("%s\n",buf);

    acc1 = gms_to_int64(22,40,11,1,0);
    acc2 = gms_to_int64(-44,40,11,0,0);

    toBin(acc1,buf);
    printf("%s\n",buf);

    toBin(acc2,buf);
    printf("%s\n",buf);

    acc1 |= acc2;
    toBin(acc1,buf);
    printf("%s\n",buf);
}


void *locate_object(nnPSchema S,nnPNode root,nnPObject obj,nnPStack stack) {
    int i, j;
    nnPChain  pc;
    nnPNode node;
    nnPBranch vc;
    nnValue v;

    node = root;

    // Varre todas as chains
    for (i=0; i<S->n_chains; i++) {
        pc = S->chains[i];

        // varre todos os niveis das chains
        for (j=0; j < pc->k; j++) {
            //
            vc = node->branches;
            while (vc) {
                // verifica se os valores são iguais
                pc->lf->get_value(obj,j,&v);
                if (pc->lf->are_equal(&v,&vc->value)) {

                    // empilha o node e o branch
                    if (stack) {
                        nn_push(stack,node);         // empilha o node
                        nn_push(stack,(nnPNode) vc); // empilha o branch
                    }

                    break;
                }
                vc = vc->next;
            }
            if (!vc) return NULL;  // Não achou o objeto
            node = vc->child;

            // ultimo elemento da chain?
            if (j+1 == pc->k) {
                // chegou a time series? Então o objeto está na lista
                if (node->terminal) {
                    if (stack) {
                        nn_push(stack,node);
                    }
                    return node->c_t_n.terminal;
                }
                node = node->c_t_n.content;
            }
        }

        node = node->c_t_n.content;
    }
    return NULL;
}


void delete_object(nnPObject obj,nnPStack stack) {
    nnPBranch vc;
    nnPNode node;
    int released;
    obj;

    released = 0;
    // Trata o no dentro da time series
    //if (remove_object_from_time_series(obj)) {
    //}

    //

    while (stack->top) {
        vc = (nnPBranch) nn_pop(stack);
        node = nn_pop(stack);

        // E filho unico?
        if ( (!vc->next) && (node->branches == vc)) {

        } else {
        }
    }
}

#define NODE_WIDTH 20
#define LINE

#if 0
int draw_it(nnPNode node,int dy) {
    int nv;
    nnPBranch pb;
    int w = 0;
    if (!node->branches) return NODE_WIDTH;

    w = 0;
    for (pb = node->branches; pb; pb = pb->next) {
        w += draw_it(pb->child,dy);
    }

    // desenha o no com filhos no meio da largura dos filhos

    // desenha na posicao
    return w;
}
void draw(nnPNode root) {

}
#endif



/******************************************************************************************//*

    Time Series
    Implementa uma lista de objetos que substitui a implantação da parte temporal
    da estrutura NanoCube

*//******************************************************************************************/
typedef struct nn_object_item {
    struct nn_object_item *next;
    nnPObject object;
} nnObjectItem, *nnPObjectItem;

#ifdef __TS__

typedef struct nn_bin {
    int bin;
    int accum;
    int count;
    struct nn_bin *next;
} nnBinNode, *nnPBinNode;
#endif

#define SIG_TEST_TERMINAL(X)  if ((X)->t.sig != SIG_TERMINAL) fatal("\nSIG ERROR: line=%d SIG:[%x] @ [%p]",__LINE__,(X)->t.sig,X);

typedef struct nn_time_series {
    nnTerminal t;

    // campos especializados
    nnPObjectItem first;
#ifdef __TS__
    nnPBinNode firstBinNode;
    nnPBinNode lastBinNode;
#endif
} nnTimeSeries, *nnPTimeSeries;


static int bin_size = 86400;

#ifdef __TS__
/*--------------------------
*
*
*
*/
int binFromTime(int seconds,int base) {
    int delta = seconds - base;
    return 1 + (delta/bin_size); // bin => 10 minutos
}
#endif

/*--------------------------
**
*
*/
int nn_query_bin(nnPTerminal pt,int tm0,int tm1) {
    nnPTimeSeries ts = (nnPTimeSeries) pt;
    nnPBinNode pbn, anchor;
    nnPObject obj = ts->first->object;
    int timeBase = obj->pclass->timeBase;

    int bin0, bin1;

    bin0 = binFromTime(tm0,timeBase);

    // localiza bin0 ou maior que ele
    for (pbn = ts->firstBinNode, anchor = NULL;
         pbn && (pbn->bin < bin0);
         anchor = pbn, pbn = pbn->next);
    if (!pbn) {
        return 0;
    } else if (pbn->bin == bin0) {

    } else {

    }

    bin1 = binFromTime(tm1,timeBase);
    while (pbn && (pbn->bin <= bin1)) {
        pbn = pbn->next;
    }
    return 1;
}

/*--------------------------
*
*
*
*/
nnPBinNode nn_create_bin_node(int bin, int accum) {
    nnPBinNode pbn = (nnPBinNode) calloc(sizeof(nnBinNode),1);
    pbn->bin = bin;
    pbn->accum = accum;
    return pbn;
}

/*--------------------------
*
*
*
*/
nnPTimeSeries nn_create_time_series(void) {
    nnPTimeSeries res;
    nn_stats.n_terminal ++;
    res =  (nnPTimeSeries) calloc(sizeof(nnTimeSeries),1);
    res->t.sig = SIG_TERMINAL;
    return res;
}

/*--------------------------
*
*
*
*/
nnPTimeSeries nn_release_time_series(nnPTimeSeries ts) {
    nnPObjectItem oi,noi;
    SIG_TEST_TERMINAL(ts);
    for (oi=ts->first;oi;oi=noi) {
        noi = oi->next;
        nn_stats.n_ref_objects --;
        nn_stats.n_content--;
        free(oi);
    }
    memset(ts,0,sizeof(nnTerminal));
    ts->t.sig = SIG_TERMINAL_REMOVED;

    free(ts);

    nn_stats.n_terminal --;
#if 0
    // remove todos os bins da TS
#endif

    return NULL;
}

/*--------------------------
*
*
*
*/
nnPTimeSeries nn_clone_time_series(nnPTimeSeries ts) {
    nnPTimeSeries ts2 = nn_create_time_series();
    nnPObjectItem oi, oi2, prev;

    SIG_TEST_TERMINAL(ts);
    SIG_TEST_TERMINAL(ts2);

    ts2->t.n = ts->t.n;
    for (oi = ts->first, prev=NULL; oi; prev = oi2, oi = oi->next) {
        oi2 = (nnPObjectItem) calloc(sizeof(nnObjectItem),1);
        oi2->object = oi->object;
        nn_stats.n_content++;
        if (prev) {
            prev->next = oi2;
        } else {
            ts2->first = oi2;
        }
    }

    return ts2;
}

/*--------------------------
*
*
*
*/
int nn_count_time_series(nnPTimeSeries ts ) {
    SIG_TEST_TERMINAL(ts);
    return ts->t.n;
}

/*--------------------------
*
*
*
*/
void nn_time_series_remove(nnPTimeSeries ts ,nnPObject obj) {
    nnPObjectItem item, prev;

    SIG_TEST_TERMINAL(ts);

#if 1
    ts->t.n --;
    nn_stats.n_content --;
    nn_stats.n_ref_objects -- ;
    return;
#endif

    for (item = ts->first, prev = NULL; item; prev = item, item = item->next) {
        if (item->object == obj) {
            ts->t.n --;
            nn_stats.n_content --;
            nn_stats.n_ref_objects -- ;
            if (prev) {
                prev->next = item->next;
            } else {
                ts->first = item->next;
            }
            item->next = NULL; // desnecessario
            return;
        }
    }

#if 0
    {
        int seconds, bin;
        nnPBinNode pbn, pbn0;

        seconds = obj->pclass->getTime(obj);
        bin = binFromTime(seconds,obj->pclass->timeBase);

        pbn = pt->firstBinNode;
        if (!pbn) return;

        for (pbn0 = NULL; pbn && bin > pbn->bin; pbn0 = pbn, pbn = pbn->next) ;

        if (!pbn) return;
        if (bin < pbn->bin) {
            Rprintf("Trying t delete a inexistent object\n");
            return;
        }

        pt->n--;

        if (!pbn0) {
            if (pbn->accum == 1) goto adjust;
            pt->firstBinNode = pbn->next;
            free(pbn);
            pbn = pt->firstBinNode;
        } else {
            if (pbn0->accum == pbn->accum - 1) goto adjust;
            pbn0->next = pbn->next;
            free(pbn);
            pbn = pbn0->next;
        }
        if (!pbn) {
            pt->lastBin = NULL;
            return;
        }
adjust:
        while (pbn) {
            pbn->accum--;
            pbn->next;
        }
    }
#endif
}


/*--------------------------
*
*
*
*/
int nn_query_time_series(nnPTimeSeries ts, nnPObject what, nnPQueryInfo qi) {
    int result;
    nnPObjectItem poi;
    nnPObject po;

    // computa o valor armazenado
    if (qi->dt_i == -1) {
        return ts->t.n;
    }

    if (qi->qtype != QUERY_DATES) {
        result = 0;
        for (poi = ts->first; poi; poi = poi->next) {
            int seconds;
            int bin;

            po = poi->object;

            seconds = po->pclass->getTime(po);
            bin = binFromTime(seconds,po->pclass->timeBase);

            if (bin < qi->dt_i) continue;
            if (bin > qi->dt_f) continue;
            result ++;
        }
    } else {
        result = 0;
        for (poi = ts->first; poi; poi = poi->next) {
            int seconds;
            int bin;
            int pos;

            po = poi->object;

            seconds = po->pclass->getTime(po);
            bin = binFromTime(seconds,po->pclass->timeBase);

            if (bin < qi->dt_i) continue;
            pos = (bin - qi->dt_i) / qi->dt_bin_size;
            if (pos >= qi->dt_n_bins) continue;
            qi->databuf[pos] ++;
            result ++;
        }
    }

    return result;
}

#define __NO_CHECK_OBJECT_EXISTENCE__

#ifdef __CHECK_OBJECT_EXISTENCE__
/*--------------------------
*
*
*
*/
int nn_object_in_time_series(nnPTerminal pt,nnPObject obj) {
    nnPObjectItem item;
    for (item = pt->first; item; item = item->next) {
        if (item->object == obj) return 1;
    }

    return 0;
}
#endif

/*--------------------------
*
*
*
*/
int nn_insert_into_time_series(nnPTimeSeries ts,nnPObject obj) {
    nnPObjectItem item;

    SIG_TEST_TERMINAL(ts);
#ifdef __CHECK_OBJECT_EXISTENCE__
    if (nn_object_in_time_series(pt,obj)) {
        trace3("--- Time Series [%p] rejecting [%p]\n",pt,obj);
        return;
    }
#endif

//    trace3("--- Time Series [%p] inserting [%p]\n",pt,obj);

#if 0
    if (pt->first) {
        nnPObject po = pt->first->object;
        int s0 = po->pclass->getTime(po);
        int s1 = obj->pclass->getTime(obj);
        if (s1 < s1) return;
    }
#endif

    nn_stats.n_ref_objects ++;
    nn_stats.n_content ++;

    item = (nnPObjectItem) calloc(sizeof(nnObjectItem),1);
    item->object = obj;
    item->next = ts->first;
    ts->first = item;
    ts->t.n ++;

#if 0
    {
        int seconds, bin;
        nnPBinNode pbn;

        seconds = obj->pclass->getTime(obj);
        bin = binFromTime(seconds,obj->pclass->timeBase);

        if (!pt->firstBinNode) {
            pbn = nn_create_bin_node(bin,1);
            pt->lastBinNode = pt->firstBinNode = pbn;

        } else if (bin == pt->lastBinNode->bin) {
            pt->lastBinNode->accum ++;

        } else if (bin > pt->lastBinNode->bin) {
            // cria um novo binNode acumulando os valores do bin anterior
            pbn = nn_create_bin_node(bin,pt->lastBinNode->accum+1);

            // coloca o novo binNode no final da lista de bins
            pt->lastBinNode->next = pbn;
            pt->lastBinNode = pbn;
        }
    }
#endif
    return ts->t.n;
}


/******************************************************************************************//*

                        QUERY for Nanocubes interface

*//******************************************************************************************/

#ifdef __QUERY__


/*--------------------------
*
*
*
*/
nnPNode  nn_seek_node(nnPNode root, nnPChain chain, nnPObject obj, int zoom) {
    int i;
    //nnValue v;
    nnPNode child;
    nnPBranch vc;
    nnPNode node;
    int ival;

    if (zoom > chain->k) return NULL;

    node = root;
    qtrace("Start Seek\n");
    //aprintf("=======\n");
    for (i = 0; i < zoom; i++) {
        ival = obj->values[chain->offset+i];
        //chain->lf->get_value(obj,i+1,&v);
        child = nn_find_child_by_ivalue(node,ival,&vc);
        qtrace("Node=%p, Child=%p, I=%d, RawValue=%d, ValueStr=%s \n",node, child, i, ival,chain->lf->object_to_string(obj,i+1,trace_buf,TRACEBUF_SZ));

        // Nao encontrou uma aresta com o valor desejado
        if (!child)  return NULL;

        node = child;
    }
    qtrace("Seek found!\n");
    return node;
}


/*--------------------------
*
*
*
*/
int nn_query_object(nnPNode root, nnPSchema S, nnPObject what, int nivel, int zoom,int dt_i,int dt_f) {
    nnPNode node;
    int result = 0;

    node = root;

    if (nivel > 0) {

        // ponto
        node = nn_seek_node(root,S->chains[0],what,zoom);
        if (!node) return 0;
        nivel --;

        if (nivel > 0) {
            // vai para a raiz da arvore do proximo chain
            node = node->c_t_n.content;
            if (!node) return 0; // erro. Tem de ter content aqui

            // busca pelo valor
            node = nn_seek_node(node,S->chains[1],what,S->chains[1]->k);
            if (!node) return 0;
        }
    }

    // vai para o node que tem a time_series
    while (!node->terminal) {
        node = node->c_t_n.content;
    }

    // pega a contagem
    // deveria usar dt_i, dt_f
    result = node->c_t_n.terminal->n;

    return result;
}


/*--------------------------
*
*
*
*/
nnPTimeSeries nn_goto_time_series(nnPNode node) {
    // vai para o node que tem a time_series
    while (!node->terminal) {
        node = node->c_t_n.content;
        if (!node) {
            fatal("Node cannot reach any Time Series");
            return NULL;
        }
    }
    return (nnPTimeSeries) (node->c_t_n.terminal);
}

#include "crimes.h"

#define ON_RIGHT_QUAD(V)     (((V) & 2)>>1)
#define ON_BOTTOM_QUAD(V)    ((V) & 1)

/*--------------------------
*
*
*
*/
int nn_do_query_other_step(
        nnPNode node, nnPSchema S, nnPObject what, int nivel, nnPQueryInfo qi,
        int xi,int yi,int len,int zoom) {
    nnPBranch vc;
    int v, result;

    // o retangulo de busca esta totalmente inscrito nos limites ?
    if ( (qi->xmin <= xi) && (xi+len <= qi->xmax) &&
            (qi->ymin <= yi) && (yi+len <= qi->ymax)) {

        qtrace("INSIDE: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [zoom:%d len:%6x] [xi: %6x ] [yi: %6x]\n",
            qi->xmin, qi->xmax, qi->ymin, qi->ymax,
            zoom, len,
            xi, yi);


    // nao está?
    } else if (zoom>0) { // analisa seus quads
        int x2, y2, len2;
        int xi_new, yi_new;
        int found = 0;

        len2 = len >> 1;
        y2 = yi + len2;
        x2 = xi + len2;

        // para cada valor encontrado, desce na arvore
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;

            if (ON_RIGHT_QUAD(v)) {
                // o maximo ficou fora deste quadrante superior?
                if (qi->xmax < x2) continue; // ignora este quad
                xi_new = x2;
            } else {
                // o minimo ficou fora deste quadrante inferior?
                if (qi->xmin > x2) continue; // ignora este quad
                xi_new = xi;
            }
            if (ON_BOTTOM_QUAD(v)) {
                // o maximo ficou fora deste quadrante superior?
                if (qi->ymax < y2) continue; // ignora este quad
                yi_new =  y2;
            } else {
                // o minimo ficou fora deste quadrante inferior?
                if (qi->ymin > y2) continue; // ignora este quad
                yi_new =  yi;
            }
            found = 1;
            qtrace("REC: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [v:%d zoom:%d len:%6x] [xi: %6x -> %6x ] [yi: %6x -> %6x]\n",
                 qi->xmin, qi->xmax, qi->ymin, qi->ymax,
                 v, zoom, len,
                 xi, xi_new, yi, yi_new);

            nn_do_query_other_step(
                vc->child, S, what, nivel,qi,
                xi_new, yi_new, len2, zoom-1);
        }
        if (!found) {
            qtrace("NOT_FOUND: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [zoom:%d len:%6x] [xi: %6x xf: %6x] [yi: %6x yf: %6x]\n",
                qi->xmin, qi->xmax, qi->ymin, qi->ymax,
                zoom, len,
                xi, xi+len, yi, yi+len);
        }
        return 1;
    } else {
        qtrace("LEAF: [xmin:%6x xmax:%6x ymin:%6x ymax:%6x] [zoom:%d len:%6x] [xi: %6x ] [yi: %6x]\n",
            qi->xmin, qi->xmax, qi->ymin, qi->ymax,
            zoom, len,
            xi, yi);

    }

    // !!! agora chegou ao nivel desejado da arvore

    // vai para o proxima sub-arvore
    node = node->c_t_n.content;
    nivel --;

    if (qi->qtype==QUERY_TOTAL) {
        result = nn_query_time_series(nn_goto_time_series(node),what,qi);
        qi->databuf[0] += result;

    // unico valor ?
    } else if (qi->value >= 0) {

        // valor procurado
        // fixado para crime
        int v0 = what->values[S->chains[1]->offset];

        // procura pelo valor
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;
            if (v == v0) break;
        }

        // nao achou o valor? retorna
        if (!vc) return 1;

        // pega o filho do ramo onde o valor foi encontrado
        node = vc->child;

        // computa as ocorrencias do valor
        result = nn_query_time_series(nn_goto_time_series(node),what,qi);

        if (qi->qtype == QUERY_VALUES) {
            // acumula o total na posicao 0
            qi->databuf[0] += result;

        } else if (qi->qtype == QUERY_DATES) {
            // ja foi processado em nn_query_time_series
        }

    // varios histograma de todos os valores
    } else {

        // contabiliza todos os valores dos nodes
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;

            // DEVE MUDAR para lidar com mais níveis

            // assume que esta no nivel desejado
            // ajustar para multiplos niveis
            result = nn_query_time_series(nn_goto_time_series(vc->child),what,qi);

            if (qi->qtype==QUERY_VALUES) {
                // acumula os resultados
                qi->databuf[v] += result;
                // printf("Values: path: %d  val: %d\n",v,result);
            } else {
                // ja foi processado em nn_query_time_series
            }
        }
    }

    return 1;

}

/*--------------------------
*
*
*
*/
int nn_do_query_location_step(
        nnPNode node, nnPSchema S, nnPObject what, int nivel, nnPQueryInfo qi,
        int row, int col, int n) {

    nnPBranch vc;
    int v, result;

    if (n>1) {
        n >>= 1;
        // para cada valor encontrado, desce na arvore
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;
            //qtrace("Location: new_n=%d, v=%d\n",n,v);
            //printf("%03d ON_RIGHT_QUAD = %d (%d->%d), ON_BOTTOM_QUAD=%d (%d->%d)\n",n,HAS_LON(v),col,col + HAS_LON(v) * n,HAS_LAT(v),row,row + HAS_LAT(v) * n);
            nn_do_query_location_step(
                vc->child, S, what, nivel,qi,
                row + ON_BOTTOM_QUAD(v) * n, col + ON_RIGHT_QUAD(v) * n,n);
        }
        return 1;
        #undef HAS_ROW
        #undef HAS_COL
    }

    // agora que chegou ao nivel desejado da arvore
    // verifica o que fazer
    node = node->c_t_n.content;
    nivel --;

    if (nivel) {
        // valor procurado
        int v0 = what->values[S->chains[1]->offset];

        // procura pelo valor
        for (vc = node->branches; vc; vc = vc->next) {
            v = vc->value.i;
            if (v == v0) break;
        }

        // nao achou o valor? retorna
        if (!vc) return 1;

        // pega o filho do ramo onde o valor foi encontrado
        node = vc->child;
    }

    // calcula o valor
    result = nn_query_time_series(nn_goto_time_series(node),what,qi);

    // coloca a contagem em row e col
    qi->databuf[row * qi->n_pontos + col] = result;

    return 1;
}

/*--------------------------
*
*
*
*/
int nn_do_query(nnPNode root, nnPSchema S, nnPObject what, int nivel, nnPQueryInfo qi) {
    nnPNode node;
    int result = 0;

    if (root) {

        if (qi->qtype == QUERY_LOCATION) {

            // acha base do processo
            node = nn_seek_node(root,S->chains[0],what,qi->zoom);
            if (!node) return 0;

            // faz a consulta
            nn_do_query_location_step(node,S,what,nivel,qi,0,0,qi->n_pontos);
        } else {
            int zoom;
            int len;

            // acha base do processo
            node = nn_seek_node(root,S->chains[0],what,qi->zoom_out);
            if (!node) return 0;

            len = 1 << (25 - qi->zoom_out);
            zoom = qi->zoom - qi->zoom_out;

            // faz a consulta
            nn_do_query_other_step(node,S,what,nivel,qi,
                qi->x_out,qi->y_out,len,zoom);

        }
    }

    return result;
}
#endif


/*=============================================================================*//*

       Crimes

*//*=============================================================================*/

typedef struct {
    int    tile_x;
    int    tile_y;

    double lat_g,lat_m,lat_s;
    double lon_g,lon_m,lon_s;
    int    tipo_crime;
    int    seconds_from_epoch;
} Crime;

typedef struct sCrimes {
    int       id;
    nnPObjectItem first, last;
    struct sCrimes *next;
} nnCrimes, *nnPCrimes;

static nnPCrimes crimes_first = NULL;
static nnPCrimes crimes_last = NULL;

/*--------------------------
*
*
*
*/
void crime_append_object(nnPCrimes pc, nnPObject po) {
    nnPObjectItem poi;

    // aloca e apenda o objeto na fila
    poi = (nnPObjectItem) calloc(sizeof(nnObjectItem),1);
    poi->object = po;
    if (pc->last)
        pc->last->next = poi;
    else
        pc->first = poi;
    pc->last = poi;

}


/*--------------------------
*
*
*
*/
int crime_gettime(nnPObject po) {
    Crime *c = (Crime *) (po->data);
    return c->seconds_from_epoch;
}

/*--------------------------
*
*
*
*/
void crime_latlon_get_value(nnPObject po, int pos, nnPValue pv) {
    Crime *p = (Crime *) (po->data);
    uint64 acc = 0;
    uint64 mask;
    int n;

    if (1) {
        mask = 0x1000000UL;
        if (pos>1) {
            mask >>= pos-1;
        }
        acc = p->tile_x & mask;
        acc <<= 1;
        acc |= p->tile_y & mask;
        n = BITS_COORD - pos;
        if (n) {
            acc >>= n;
        }

    } else {
        // converte a latitude para bits da quad-tree
        acc = gms_to_int64(p->lat_g,p->lat_m,p->lat_s,0,0);
        acc = gms_to_int64(p->lon_g,p->lon_m,p->lon_s,1,acc);

        // seleciona a parte que efetivamente sera utilizada
        pos --;
        mask = 0x0003000000000000UL;
        n = (BITS_COORD - 1) - pos;
        while (pos > 0) {
            mask >>= 2;
    //        mask = mask | 0xC000000000000000UL;
            pos --;
        }
        acc = acc & mask;
        while (n > 0) {
            acc >>= 2;
    //        mask = mask | 0xC000000000000000UL;
            n --;
        }

    }
    pv->i = (int) acc;
}

/*--------------------------
*
*
*
*/
int crime_latlon_equal_values(nnPValue v1, nnPValue v2) {
    return v1->i == v2->i;
}

/*--------------------------
*
*
*
*/
char *crime_latlon_value_to_string(nnPValue v,int pos, char *buf,int maxlen) {
    uint64 bits, val;
    char *map[] = { "00.", "01.", "10.", "11." };
    *buf = '\0';

    val = v->i;
    do {
        bits = ( val & 0x0003000000000000UL );
        val <<= 2;
        bits >>= 48;
        strncat(buf,map[bits],maxlen);
        pos --;
    } while (pos > 0);

    return buf;
}


/*--------------------------
*
*
*
*/
char *crime_latlon_to_string(nnPObject o,int pos, char *buf,int maxlen) {
    nnValue v;
    *buf = '\0';
    if (!o) return buf;
    crime_latlon_get_value(o,pos,&v);
    return crime_latlon_value_to_string(&v,pos,buf,maxlen);
}

/*--------------------------
*
*
*
*/
void crime_get_value(nnPObject po, int pos, nnPValue pv) {
    Crime *p = (Crime *) (po->data);

    pos;
    pv->i = p->tipo_crime;
}

/*--------------------------
*
*
*
*/
int crime_are_equal_values(nnPValue v1, nnPValue v2) {
    return v1->i == v2->i;
}

/*--------------------------
*
*
*
*/
char *crime_value_to_string(nnPValue v,int pos, char *buf,int maxlen) {
    pos;
    buf[3] = '\0';
    // ajustar
    sprintf(buf,"%02d",v->i);
    //strncat(buf,v->i == 0? "Crime0" : "Crime1",maxlen);
    return buf;
}

/*--------------------------
*
*
*
*/
char *crime_to_string(nnPObject o,int pos, char *buf,int maxlen) {
    nnValue v;
    crime_get_value(o,pos,&v);
    return crime_value_to_string(&v,pos,buf,maxlen);
}


/*--------------------------
*
*
*
*/
char *parseInt(char *p0, int *v,char sep) {
    char *p = strchr(p0,sep);
    if (!p) return NULL;
    *p = '\0';
    *v = atoi(p0);
    return p;
}

static nnPSchema crimes_S;
static nnPClass crimes_class;
static nnPNode crimes_nano_cube ;

/*--------------------------
*
*
*
*/
void printBits(int v, int n) {
    int mask = 1 << (n-1);
    while (mask) {
        printf("%c",(v&mask)?'1':'0');
        mask >>=1;
    }
}

/*--------------------------
*
*
*
*/
void printTiles(int x, int y, int n) {
    int mask = 1 << (n-1);
    int valx = 0;
    int valy = 0;
    while (mask) {
        valx = valx << 1 | ((x & mask)?1:0);
        valy = valy << 1 | ((y & mask)?1:0);
        if (500 < valx && valx < 1200)
            printf("(%d,%d) ",valx,valy);
        mask >>=1;
    }
}

/*--------------------------
*
*
*
*/
void crime_print(nnPObject o) {
    int i;
    nnValue v;
    for (i = 0; i < BITS_COORD; i++) {
        crime_latlon_get_value(o,i+1, &v);
        printf("%d ",v.i);
    }
    crime_get_value(o,i+1, &v);
    printf("- %d ",v.i);
    printf("\n");
}

/*--------------------------
*
*
*
*/
int crime_query_ex(double lat1,double lon1,double lat2,double lon2,nnPQueryInfo qi) {
    Crime c;
    nnPObject po;
    int seconds = 0;
    int nivel;
    int n;

    /*
    if (lat2 > lat1) { double x; x = lat1; lat1 = lat2; lat2 = x; }
    if (lon2 > lon1) { double x; x = lon1; lon1 = lon2; lon2 = x; }
    */
    c.tile_x = (int) lon1 << (BITS_COORD - qi->zoom);
    c.tile_y = (int) lat1 << (BITS_COORD - qi->zoom);

#if 0
    printTiles(c.tile_x,c.tile_y,BITS_COORD);
    printf("\n");
#endif

    // zoom += shift_log2(n_pontos);
    coord_to_gms(lat1,&c.lat_g,&c.lat_m,&c.lat_s);
    coord_to_gms(lon1,&c.lon_g,&c.lon_m,&c.lon_s);

    c.seconds_from_epoch = seconds;
    c.tipo_crime = qi->value;

    if (qi->qtype == QUERY_LOCATION) {
        qi->count = qi->n_pontos * qi->n_pontos;
        if (qi->count > qi->datalen) qi->count = qi->datalen;
        n = qi->count;

        nivel = (qi->value == -1)? 1: 2;

    } else {
        unsigned int mask = 0x1 << (BITS_COORD-1);
        int zoom = 0;

        int xres, yres;
        qi->ymin = lat2tiley(lat2,BITS_COORD);
        qi->ymax = lat2tiley(lat1,BITS_COORD) + 1;

        if (qi->ymin > qi->ymax) {
            int tmp = qi->ymin;
            qi->ymin = qi->ymax;
            qi->ymax = tmp;
            // printf("LAT INVERTIDO\n");
            // return 0;
        }

        qi->xmin = long2tilex(lon1,BITS_COORD);
        qi->xmax = long2tilex(lon2,BITS_COORD) + 1;

        if (qi->xmin > qi->xmax) {
            int tmp = qi->xmin;
            qi->xmin = qi->xmax;
            qi->xmax = tmp;
            // printf("LON INVERTIDO\n");
            // return 0;
        }

        c.tile_x = qi->xmin;
        c.tile_y = qi->ymin;

        // faz o xor entre as coordenadas e acha o primeiro bit divergente
        xres = qi->xmin ^ qi->xmax;
        yres = qi->ymin ^ qi->ymax;

        zoom = 1;
        while ( (!(yres & mask)) && (!(xres & mask))) {
            mask >>= 1;
            zoom ++;
        }

        // limpa o resto dos tiles originais
        while (mask) {
            unsigned int nmask = ~ mask;
            c.tile_x &= nmask;
            c.tile_y &= nmask;
            mask >>= 1;
        }

        qi->x_out = c.tile_x;
        qi->y_out = c.tile_y;

        zoom --;
        if (zoom >= qi->zoom) zoom = qi->zoom;
        // if (zoom < 0) zoom = 0;
        qi->zoom_out = zoom;

        if (qi->qtype == QUERY_VALUES) {
            if (qi->value == -1) {
                qi->count = 128;
            } else {
                qi->count = 1;
            }
            n = qi->count;
            qi->n_pontos = 1;
            nivel = (qi->value == -1)? 1: 2;

        } else if (qi->qtype == QUERY_DATES) {
            qi->count = qi->dt_n_bins;
            n = qi->count;
            qi->n_pontos = 1;
            nivel = (qi->value == -1)? 1: 2;

        } else if (qi->qtype == QUERY_TOTAL) {
            qi->count = 1;
            n = 1;
            qi->n_pontos = 1;
            nivel = 1;
        }
    }

    memset(qi->databuf,0,sizeof(int) * n);

    //
    po = nn_create_object(crimes_class,&c);

    nn_prepare_object_values(po, crimes_S);


    // printf("------------\n");
    allow_print=1;
    qtrace("Query Type = %d: Object Values =",qi->qtype);
    // print_values(po,BITS_COORD);

    nn_do_query(crimes_nano_cube, crimes_S, po, nivel, qi);
    allow_print=0;

    nn_release_object(po);
    return 1;
}

nnPTerminal ts_create(void) { return (nnPTerminal) nn_create_time_series(); }
nnPTerminal ts_clone(nnPTerminal pt) { return (nnPTerminal) nn_clone_time_series((nnPTimeSeries) pt); }
nnPTerminal ts_release(nnPTerminal pt) { return (nnPTerminal) nn_release_time_series( (nnPTimeSeries) pt); }

int  ts_insert (nnPTerminal pt,nnPObject obj) { return nn_insert_into_time_series((nnPTimeSeries) pt,obj); }
void ts_remove (nnPTerminal pt,nnPObject obj) { nn_time_series_remove((nnPTimeSeries) pt,obj); }

int  ts_count (nnPTerminal pt) { return nn_count_time_series((nnPTimeSeries) pt); }

static ITerminal iterminal = {
    //nnPTerminal (*create)   (void);
    ts_create,

    //nnPTerminal (*clone)    (nnPTerminal pt);
    ts_clone,

    //nnPTerminal (*release)  (nnPTerminal pt);
    ts_release,

    //void        (*insert)   (nnPTerminal pt,nnPObject obj);
    ts_insert,

    //void        (*remove)   (nnPTerminal pt,nnPObject obj);
    ts_remove,

    //int    (*count)    (nnPTerminal pt);
    ts_count,

    /* interface para Nanocubes */
    //int         (*query)    (nnPTerminal pt, nnPObject obj, void* query);
    NULL
} ;





//**********************************************************************************************************



/*--------------------------
*
*
*
*/
int prepare_crimes(nnPNode *nano_cube,nnPSchema *S, nnPClass *classCrime) {
    struct tm t;
    int seconds;
    nnPChain latlon_chain;
    nnPChain crime_chain;
    //nnPSchema S;
    //nnPClass classCrime;
    //nnPNode nano_cube ;

    // Cria as chains para as dimensoes
    latlon_chain = nn_create_chain(BITS_COORD,
                                   nn_create_label_function(
                                      crime_latlon_get_value,
                                      crime_latlon_equal_values,
                                      crime_latlon_to_string,
                                      crime_latlon_value_to_string));

    crime_chain = nn_create_chain(1,
                                   nn_create_label_function(
                                        crime_get_value,
                                        crime_are_equal_values,
                                        crime_to_string,
                                        crime_value_to_string));

    // Cria a label Function para time
    // lf_time = nn_create_label_function(NULL,NULL,NULL,NULL);



    // Cria o schema para o teste
    *S = nn_create_schema(2);
    nn_schema_set_chain(*S,1,latlon_chain);
    nn_schema_set_chain(*S,2,crime_chain);
    (*S)->pit = &iterminal;

    // Cria a classe
    t.tm_year = 2001-1900; t.tm_mon = 1-1; t.tm_mday = 1;
    t.tm_hour = 0; t.tm_min = 0; t.tm_sec = 0; t.tm_isdst = -1;
    seconds = (int) mktime(&t);

    {
        int s2;
        // 06/12/2013
        t.tm_year = 2013-1900; t.tm_mon = 6-1; t.tm_mday = 12;
        t.tm_hour = 0; t.tm_min = 0; t.tm_sec = 0; t.tm_isdst = -1;
        s2 = (int) mktime(&t);
        s2 /= 86400;
        s2 -= seconds/86400;
        s2 += 0;
    }


    *classCrime = nn_create_class_ex(sizeof(Crime),(*S)->n_values,(*S)->n_chains,crime_gettime,seconds);

    *nano_cube = nn_create_node();

    return 1;
}


/*--------------------------
*
*
*
*/
void load_crimes(FILE *f,nnPNode nano_cube,nnPSchema S,nnPClass classCrime,nnPCrimes pc) {
    char line[1024];
    char *p0,*p;

    double lat, lon;
    struct tm t;
    int seconds;
    int id;
    Crime c;
    nnPObject po;
    int count;

    count = 0;
    // le os pontos do arquivo
    while (fgets(line,1023,f)) {

        p0 = line;
        if (line[0]=='-' && line[1]=='-') break;
        // lat
        lat = strtod(p0,&p);
        p0 = p+1;

        // lon
        lon = strtod(p0,&p);
        p0 = p+1;

        // type
        p = parseInt(p0,&id,';');
        if (!p) return;

        // parse date
        if (!(p = parseInt(p+1,&t.tm_mday,'/'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_mon,'/'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_year,' '))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_hour,':'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        if (!(p = parseInt(p+1,&t.tm_min,';'))) {
            printf("Parsing Date: %s\n", line);
            return;
        }
        t.tm_mon --;
        t.tm_year -= 1900;
        t.tm_sec = 0;
        t.tm_isdst = 0;
        seconds = (int) mktime(&t);
        //printf("%02d-%02d-%02d %02d:%02d %12d  %6d\n",t.tm_year,t.tm_mon,t.tm_mday,t.tm_hour,t.tm_min,seconds,(seconds-classCrime->timeBase)/86400);

        // prepara o objeto
        coord_to_gms(lat,&c.lat_g,&c.lat_m,&c.lat_s);
        coord_to_gms(lon,&c.lon_g,&c.lon_m,&c.lon_s);

        c.tile_x = long2tilex(lon,BITS_COORD);
        c.tile_y = lat2tiley(lat,BITS_COORD);

        c.tipo_crime = id;
        c.seconds_from_epoch = seconds;
        po = nn_create_object(classCrime,&c);
#if 0
        if (count % 50 == 0) {
            int i;
            nnValue v;
            allow_print = 1;
            nn_prepare_object_values(po,crimes_S);
            print_values(po,BITS_COORD);

            printf("X = "); printBits(c.tile_x,BITS_COORD);
            printf("\n");
            printf("Y = "); printBits(c.tile_y,BITS_COORD);
            printf("\n");
            printTiles(c.tile_x,c.tile_y,BITS_COORD);
            printf("\n");

            for (i=0;i<BITS_COORD;i++) {
                crime_latlon_get_value(po,i+1,&v);
                qtrace("%02d ", v.i);
            }
            printf("\n");
            allow_print = 0;
        }
#endif
        crime_append_object(pc,po);
        xprintf("\n~~~~~~~~~~~~~~~~~~~~~~\n");
        nn_add_to_nano_cube_ex(nano_cube,S,po);
        // check_sums(crimes_nano_cube,crimes_S,1,0,po);
        count++;
    }

#if 0
    {
        int n = 1;
        for (po = crimes_class->first; po, n<30; po=po->next, n++) {
            printf("Deletando %d => ",n);
            nn_del_from_nano_cube(crimes_nano_cube,crimes_S,1,po);
            printf("\n");
        }
    }
#endif

    printf("Itens inseridos: %d\n",count);
    fclose(f);
}

/*=============================================================================*//*

       Crimes

*//*=============================================================================*/

/*--------------------------
*
*
*
*/
int nc_append_crimes(char *path) {
    int id;
    char fname[200];
    FILE *f;
    nnPCrimes pc;

    if (!crimes_last) {
        id = 1;
    } else {
        id = crimes_last->id + 1;
    }
//    path = "data";

    sprintf(fname,"%s\\crime50k_%06d.txt",path,id);
    fopen_s(&f,fname,"rt");
    if (!f) return(0);

    pc = (nnPCrimes) calloc(sizeof(nnCrimes),1);
    pc->id = id;

    // encadeia no final da lista
    if (crimes_last)
        crimes_last->next = pc;
    else
        crimes_first = pc;
    crimes_last = pc;

    printf("---------------------------------------------------------\n");
    printf("Inserindo itens do arquivo: %s\n",fname);
    load_crimes(f,crimes_nano_cube,crimes_S,crimes_class,pc);

    return 1;
}


/*--------------------------
*
*
*
*/
void nc_remove_crimes(char *path) {
    char fname[200];
    int i;
    //int id;
    //FILE *f;

    nnPCrimes pc = crimes_first;
    nnPObjectItem poi;

    if (!pc) return;

    if (!pc->first) fatal("Erro 120\n");

    sprintf(fname,"%s\\crime50k_%06d.txt",path,pc->id);
    //fopen_s(&f,fname,"rt");
    //if (!f) return(0);

    printf("---------------------------------------------------------\n");
    //check_sums(crimes_nano_cube,crimes_S,1,0,NULL);
    printf("Removendo lotes de itens do arquivo %s\n",fname);
    i = 0;
    do {
        poi = pc->first;
        pc->first = poi->next;
        if (!pc->first) pc->last = NULL;

        if (!poi) fatal("Erro 110\n");

        Rprintf("I=%d\n",i);i++;
        crimes_nano_cube = nn_remove_from_nanocubes(crimes_nano_cube,crimes_S,poi->object);
        check_sums(crimes_nano_cube,crimes_S,1,0,poi->object);
        free(poi);
        //break;
    } while (pc->first);

#ifdef __STATS__
    show_stats();
#endif

    // passa para o proximo crimes
    crimes_first = pc->next;
    if (!crimes_first) crimes_last = NULL;
    free(pc);
}


/*--------------------------
*
*
*
*/
void enqueue_crimes(nnPCrimes pc) {
    if (!crimes_last) {
        crimes_first = pc;
    }
    pc->next = crimes_last;
    crimes_last = pc;
}

/*--------------------------
*
*
*
*/
void dequeue_crimes(void) {
    nnPCrimes pc;

    if (!crimes_first) return;
    pc = crimes_first;
    crimes_first = pc->next;
    if (!crimes_first) crimes_last = NULL;
}

#define NFILES_IN 10
#define NFILES_OUT 1

/*--------------------------
*
*
*
*/
void carrega_crimes(char *path) {
    int n = NFILES_IN;
    clock_t start;
    int ms;
    long int mem;
    char *suf = "Bytes";

    start = clock();
    while (n) {
        nc_append_crimes(path);
        n--;
    }
    ms = (clock()-start);

    mem = nn_stats.n_ref_objects * sizeof(nnObjectItem) ;
    mem += nn_stats.n_objects * (crimes_class->size);

    if (mem > 10024) {
        mem /= 1024;
        suf = "KB";
    }

    if (mem > 10024) {
        mem /= 1024;
        suf = "MB";
    }

    printf("---------------------------------------\nAfter load:\n");
    printf("Tempo (ms): %d \n",ms);
#ifdef __STATS__
    printf("---------------------------------------\n");
    show_stats();
#endif


//    load_crimes(fname1,crimes_nano_cube,crimes_S,crimes_class);
//    load_crimes(fname2,crimes_nano_cube,crimes_S,crimes_class);

#ifdef __OLD_DUMP__
      smart_dump(nano_cube,S,"");
#endif
}

/*--------------------------
*
*
*
*/
void descarrega_crimes(char *path) {
    int n = NFILES_OUT;
    clock_t start;
    int ms;
    long int mem;
    char *suf = "Bytes";

    Rprintf("Descarregando a estrutura de dados ...\n");
    start = clock();
    while (n) {
        nc_remove_crimes(path);
        n--;
    }
    ms = (clock()-start);

    mem = nn_stats.n_ref_objects * sizeof(nnObjectItem) ;
    mem += nn_stats.n_objects * (crimes_class->size);

    if (mem > 10024) {
        mem /= 1024;
        suf = "KB";
    }

    if (mem > 10024) {
        mem /= 1024;
        suf = "MB";
    }

    printf("---------------------------------------\nAfter unload:\n");
    printf("Tempo (ms): %d\n",ms);
#ifdef __STATS__
    printf("---------------------------------------\n");
    show_stats();
#endif


//    load_crimes(fname1,crimes_nano_cube,crimes_S,crimes_class);
//    load_crimes(fname2,crimes_nano_cube,crimes_S,crimes_class);

#ifdef __OLD_DUMP__
      smart_dump(nano_cube,S,"");
#endif
}

#define __TESTE__

/*--------------------------
*
*
*
*/
void crime_init(int bin) {
    char *dir = "data";
    memset(&nn_stats,0,sizeof(nn_stats));

    bin_size = 3600;

    printf("Inicializando a estrutura de dados ...\n");
    if (!prepare_crimes(&crimes_nano_cube,&crimes_S,&crimes_class)) {
        printf("Failing to prepare\n");
        return;
    };

#ifndef __TESTE__
    carrega_crimes(dir);
#else
    carrega_crimes(dir);
    check_sums(crimes_nano_cube,crimes_S,1,0,NULL);

    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);

    carrega_crimes(dir);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);
    check_sums(crimes_nano_cube,crimes_S,1,0,NULL);
    //check_terminal_node(crimes_nano_cube,crimes_S,1,0);
    descarrega_crimes(dir);

#if 0
    carrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    carrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
    descarrega_crimes(dir);
#endif
    printf("done\n");
    exit(0);
#endif


//        carrega_crimes("ponto1.csv","ponto2.csv");
}

/*--------------------------
*
*
*
*/
void main__(int argc, char *argv[] ) {
    crime_init(86400);
}


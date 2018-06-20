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

typedef struct tn_node {
    int sig;
    struct tn_branch *branches;

#ifdef __NOVO__
    int               counter;    // registra quantos objetos/paths esta "time_series" acumula
#else
    union {
        struct tn_branch *branches;   // lista de valores deste node
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
        struct tn_node        *content;
        struct tn_terminal    *terminal;
        struct tn_node        *next;
    } c_t_n;

} nnNode, *nnPNode;

typedef struct {
    nnPNode first;
} nnNodesToFree, *nnPNodesToFree;

#define SIG_BRANCH  0x63829153

typedef struct tn_branch {
    struct tn_branch *next;

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
nnPNode tn_create_node(void) {
    nnPNode res =  (nnPNode) calloc(sizeof(nnNode),1);
    res->sig = SIG_NODE;
    tn_stats.n_nodes++;
    return res;
}

/*--------------------------
*
*
*
*/
nnPNode tn_release_node(nnPNode node) {
    SIG_TEST(NODE,node);
    node->branches = NULL;
    tn_stats.n_nodes--;
    free(node);
    return NULL;
}


/*--------------------------
*
*
*
*/
nnPNode tn_node_to_free(nnPNodesToFree list, nnPNode node, nnPNode redirect) {
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
nnPBranch tn_create_value_child(void) {
    nnPBranch vc =  (nnPBranch) calloc(sizeof(nnBranch),1);
    vc->sig = SIG_BRANCH;
    tn_stats.n_edges ++;
    return vc;

}

/*--------------------------
*
*
*
*/
nnPBranch tn_release_value_child(nnPBranch p) {
    SIG_TEST(BRANCH,p);
    tn_stats.n_edges--;
    memset(p,0,sizeof(nnBranch));
    free(p);
    return NULL;
}


#ifndef __OBJECT_H
#define  __OBJECT_H

#include "types.h"

/******************************************************************************************//*

    nnObject, nnClass

    Assume que todos os objetos a serem indexados são estruturalmente iguais
    Note que os objetos poderiam ter estruturas completamente diferentes
    sendo similares apenas pelas funções, como duck typing.

*//******************************************************************************************/

#define SIG_OBJECT  0x48282134

typedef struct tn_object {
  int sig;
  struct tn_class  *pclass;          // Classe do Objeto, ver abaixo
  struct tn_object *next;            // encadeamento na lista de todos os objetos
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

typedef struct tn_class {
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

#endif

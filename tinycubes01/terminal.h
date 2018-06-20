#ifndef __TERMINAL_H
#define __TERMINAL_H

#include "types.h"

/******************************************************************************************//*

    ITerminal

*//******************************************************************************************/

#define SIG_TERMINAL  			0x44219333
#define SIG_TERMINAL_REMOVED 	0x7A289818

typedef struct tn_terminal {
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


nnPTerminal terminal_create(PITerminal pit);
nnPTerminal terminal_clone(PITerminal pit, nnPTerminal pt);
nnPTerminal terminal_release(PITerminal pit, nnPTerminal pt);
int terminal_insert (PITerminal pit, nnPTerminal pt, nnPObject obj);
void 		terminal_remove (PITerminal pit, nnPTerminal pt, nnPObject obj);
int  		terminal_count (PITerminal pit, nnPTerminal pt);


#endif

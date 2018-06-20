#include "terminal.h"

nnPTerminal terminal_create(PITerminal pit) {
    //tn_stats.n_terminal ++;
    return pit->create();
}

nnPTerminal terminal_clone(PITerminal pit, nnPTerminal pt) {
    //tn_stats.n_terminal += pit->count(pt);
    return pit->clone(pt);
}

nnPTerminal terminal_release(PITerminal pit, nnPTerminal pt) {
    //tn_stats.n_terminal --;
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


# encoding: utf-8
# author: Thiago da Cunha Borges

import getopt

import servidor
import json
import math
import re
import socket
import socketserver
import sys
from time import sleep

from tkinter import Frame, Button
from threading import Thread
from os import system, path, listdir


class Tinycubes:
    host = '127.0.0.1'
    port = 23456
    my_socket = socket.socket()

    def __init__(self, bin='', start=True):
        self.bin = bin
        self.start = start
        self.conectado = False
        try:
            for i in range(2):
                t = Thread(target=self.thread, args=(i,))
                t.start()
        except Exception as e:
            print(e)
            sys.exit(1)

    def thread(self, n):
        if n == 0 and self.start:
            if self.bin != '':
                system('nc.exe -bin {}'.format(self.bin))
            else:
                system('nc.exe')
        elif n == 1:
            print('Conectando ao tinycubes', end='.')
            self.conecta()

    def conecta(self):
        try:
            self.my_socket.connect((self.host, self.port))
            print('\nTinycubes conectado na porta {}'.format(self.port))
            self.conectado = True
        except ConnectionRefusedError:
            sleep(0.5)
            print(end='.')
            self.conecta()

    def get_response(self, msg=''):
        msg += '\r\n'
        try:
            self.my_socket.send(msg.encode())
            data = self.my_socket.recv(1024)
            info = data.decode()
            while data[-2:] != b'\r\n':
                data = self.my_socket.recv(1024)
                info += data.decode()
            return info
        except Exception as e:
            print('Não foi possível se conectar ao servidor.\n' + str(e))
            self.conecta()
            return self.get_response(msg)

    def send_message(self, msg):
        msg += '\r\n'
        try:
            self.my_socket.send(msg.encode())
        except Exception as e:
            print('Não foi possível se conectar ao servidor.\n' + str(e))
            self.conecta()
            self.send_message(msg)

    def close(self):
        self.my_socket.close()


tinycubes_bin = '3600'
tinycubes_start = True
if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hwb:", ["help", "wait", "bin="])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)
    for option, arg in opts:
        if option in ('-b', '--bin'):
            tinycubes_bin = arg
        elif option in ('-w', '--wait'):
            tinycubes_start = False
        elif option in ('-h', '--help'):
            print('Use:')
            print('-b <value> or --bin <value>\t\t\t to change bin')
            print('-w or --wait\t\t\t\t to start the tinycubes separately and wait for the connection')
            sys.exit(0)

TINYCUBES = Tinycubes(bin=tinycubes_bin, start=tinycubes_start)


class Schema:
    def __init__(self):
        self.schema = {
            "fields": [
                {"name": "location", "type": "nc_dim_quadtree_25", "valnames": {}},
                {"name": "Primary_Type", "type": "nc_dim_cat_1", "valnames": self.read_data()},
                {"name": "Date", "type": "nc_dim_time_2", "valnames": {}},
                {"name": "count", "type": "nc_var_uint_4", "valnames": {}}
            ],
            "metadata": [
                {"key": "location__origin", "value": "degrees_mercator_quadtree25"},
                {"key": "tbin", "value": "2001-01-01_00:00:00_3600s"},
                {"key": "name", "value": "Chicago_Crime"}
            ]
        }

    @staticmethod
    def read_data():
        kind = {}
        # coords = {'lat': [], 'long': {}}
        # time = []

        def text_files():
            caminho = [path.join('data', nome) for nome in listdir('data')]
            txt_files = [f for f in caminho if f.lower().endswith('.txt')]
            return txt_files

        for file_name in text_files():
            with open(file_name) as file:
                for line in file.readlines():
                    info = line.replace('\n', '').split(';')
                    if info[4] not in kind.keys():
                        kind[info[4]] = int(info[2])
                    else:
                        if int(info[2]) != kind.get(info[4]):
                            print('Erro ao ler os arquivos.')
                            print('Dados inconsistente.\nTipos com valores diferentes.\nSaindo')
                            print('Tipo {} com valores {} e {}'.format(info[4], kind.get(info[4]), info[2]))
                            print('Arquivo: {} com dado inconsistente'.format(file_name))
                            sys.exit(1)
                    # coords['lat'].append(float(info[0]))
                    # coords['long'].append(float(info[1]))
                    # time.append(info[3])
        return kind

    def __str__(self):
        return json.dumps(self.schema)


class Interface(Frame):

    def __init__(self, master=None):
        Frame.__init__(self, master)

        self.add_buttom = Button(self, text='Adicionar', command=self.add, font='Arial 24 bold')
        self.add_buttom.pack(side='left', expand=True, fill='both')
        self.remove_buttom = Button(self, text='Remover', command=self.remove, font='Arial 24 bold')
        self.remove_buttom.pack(side='left', expand=True, fill='both')
        self.pack(side='top', expand=True, fill='both')
        self.master.title('Tinycubes')
        self.master.iconbitmap('favicon.ico')

    @staticmethod
    def add():
        TINYCUBES.send_message('+')

    @staticmethod
    def remove():
        TINYCUBES.send_message('-')


class Requisicao:
    schema = Schema()

    def __init__(self, request):
        self.request = request
        if self.request[1:] == 'schema':
            self.tipo = 'schema'
        elif self.request[1:6] == 'count':
            self.tipo, self.identidade = self.identifica_contagem()
            self.msg = self.mensagem()

    def identifica_contagem(self):

        def ytile_complement(ytile, zoom):
            n = 2.0 ** zoom - 1
            return n - ytile

        def num2deg(xtile, ytile, zoom):
            n = 2.0 ** zoom
            lon_deg = xtile / n * 360.0 - 180.0
            lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
            lat_deg = -1 * math.degrees(lat_rad)
            return lat_deg, lon_deg

        def dive(parametros):
            busca = re.search(r'(.*)\((.*),(.*),(.*)\),(.*)', parametros)
            if busca:
                grupos = busca.groups()
                lat_n, long_o = num2deg(int(grupos[1]), int(grupos[2]), int(grupos[3]))
                lat_s, long_l = num2deg(int(grupos[1]) + 1, int(grupos[2]) + 1, int(grupos[3]))
                ytile = ytile_complement(int(grupos[2]), int(grupos[3]))
                return {
                    'tipo': grupos[0],
                    'xtile': int(grupos[1]),
                    'ytile': int(ytile),
                    'lat_n': lat_n,
                    'long_o': long_o,
                    'lat_s': lat_s,
                    'long_l': long_l,
                    'zoom': int(grupos[3]),
                    'quantidade_pontos': 2 ** int(grupos[4])
                }
            else:
                return ''

        def mercartor_mask(parametros):
            parametros = parametros.replace('%22', '')
            pontos = parametros.split(',')
            zoom = int(pontos.pop())
            lat = []
            long = []
            j = 0
            for ponto in pontos:
                if j % 2 == 0:
                    lat.append(int(((float(ponto) + 1) / 2) * (2 ** zoom)))
                else:
                    long.append(int(((float(ponto) + 1) / 2) * (2 ** zoom)))
                j += 1
            norte, oeste = num2deg(max(lat), max(long), zoom)
            sul, leste = num2deg(min(lat), min(long), zoom)
            return [norte, oeste, sul, leste, zoom - 8]

        def tipo_contagem(identificacao, tipo):
            vals = {}
            if identificacao == 'r':
                try:
                    parametros = re.search(r'\(%22(.*)%22,(.*)\((.*)\).*\)', tipo).groups()
                except Exception as e:
                    print(e)
                    parametros = ['1', '2', '3', '4']
                vals['tipo'] = parametros[0]
                if parametros[1] == 'mercator_mask':
                    vals[parametros[1]] = mercartor_mask(parametros[2])
                else:
                    vals[parametros[1]] = parametros[2].replace('%22', '').split(',')
            elif identificacao == 'a':
                parametros = re.search(r'\(%22(.*)%22,(\w*)\((.*)\)(.*)\)', tipo).groups()
                vals['tipo'] = parametros[0]
                vals[parametros[1]] = dive(parametros[2])
                vals['img'] = parametros[3]
            return vals

        req = self.request[6:]
        if req == '':
            return 'total', []
        contagens = re.split(r'\.([a|r])', req)[1:]
        for t in range(0, len(contagens) // 2):
            contagens[t] += contagens[t + 1]
            del contagens[t + 1]
        ids = []
        valores = []
        for contagem in contagens:
            if contagem[0] == 'r':
                ids.append('r')
            elif contagem[0] == 'a':
                ids.append('a')
            valores.append(tipo_contagem(contagem[0], contagem[1:]))

        # requisição das datas do schema
        if len(valores) == 1 and valores[0]['tipo'] == 'Date':
            return 'schema_date', valores

        for i in range(len(valores)):
            if valores[i]['tipo'] == 'Date' and valores[i].get('mt_interval_sequence', False):
                # requisicação de tempo
                return 'date', valores

            for key, val in valores[i].items():
                if ids[i] == 'a' and val == 'location':
                    # requisição de localização no mapa
                    return 'location', valores

                elif ids[i] == 'a' and val == 'Primary_Type':
                    # requisição de tipos
                    return 'category', valores

        # requisição de totais
        return 'total', valores

    def mensagem(self):
        if self.tipo == 'schema':
            return 'S'
        else:
            date, location, category = '', '', ''
            # Location
            if self.tipo == 'location':
                for ids in self.identidade:
                    if ids.get('tipo') == 'Date':
                        date = 'D ' + ids.get('interval')[0] + ' ' + ids.get('interval')[1] + ' '
                    elif ids.get('tipo') == 'Primary_Type':
                        category = 'V ' + ids.get('set')[0] + ' '
                    elif ids.get('tipo') == 'location':
                        location = str(ids['dive']['ytile']) + ' ' + str(ids['dive']['xtile']) + ' '
                        location += str(ids['dive']['lat_s']) + ' ' + str(ids['dive']['long_l']) + ' '
                        location += str(ids['dive']['zoom']) + ' ' + str(ids['dive']['quantidade_pontos']) + ' '
                return 'L ' + location + date + category

            # Data_Schema
            elif self.tipo == 'schema_date':
                return 'D -85.05 -179.8 85.05 179.8 1 ' + 'I ' \
                           + self.identidade[0]['mt_interval_sequence'][0] + ' ' \
                           + self.identidade[0]['mt_interval_sequence'][1] + ' ' \
                           + self.identidade[0]['mt_interval_sequence'][2]

            # Date
            elif self.tipo == 'date':
                for ids in self.identidade:
                    if ids.get('tipo') == 'location':
                        location = str(ids['mercator_mask'][0]) + ' ' + str(ids['mercator_mask'][1]) + ' '
                        location += str(ids['mercator_mask'][2]) + ' ' + str(ids['mercator_mask'][3]) + ' '
                        location += str(ids['mercator_mask'][4]) + ' '
                    elif ids.get('tipo') == 'Date':
                        date = 'I ' + str(ids['mt_interval_sequence'][0]) + ' ' + str(ids['mt_interval_sequence'][1])
                        date += ' ' + str(ids['mt_interval_sequence'][2]) + ' '
                    elif ids.get('tipo') == 'Primary_Type':
                        category = 'V ' + str(ids['set'][0]) + ' '
                return 'D ' + location + date + category

            # Tipo
            elif self.tipo == 'category':
                for ids in self.identidade:
                    if ids.get('tipo') == 'location':
                        location = str(ids['mercator_mask'][0]) + ' ' + str(ids['mercator_mask'][1]) + ' '
                        location += str(ids['mercator_mask'][2]) + ' ' + str(ids['mercator_mask'][3]) + ' '
                        location += str(ids['mercator_mask'][4]) + ' '
                    elif ids.get('tipo') == 'Date':
                        date = 'D ' + str(ids['interval'][0]) + ' ' + str(ids['interval'][1]) + ' '
                return 'V ' + location + date + category

            # Total
            elif self.tipo == 'total':
                for ids in self.identidade:
                    if ids.get('tipo') == 'location':
                        location = str(ids['mercator_mask'][0]) + ' ' + str(ids['mercator_mask'][1]) + ' '
                        location += str(ids['mercator_mask'][2]) + ' ' + str(ids['mercator_mask'][3]) + ' '
                        location += str(ids['mercator_mask'][4]) + ' '
                    elif ids.get('tipo') == 'Date':
                        date = 'D ' + str(ids['interval'][0]) + ' ' + str(ids['interval'][1]) + ' '
                    elif ids.get('tipo') == 'Primary_Type':
                        category = 'V ' + str(ids['set'][0]) + ' '
                return 'T ' + location + date + category

            else:
                return 'T 10 10 10 10 10'

    def __str__(self):
        if self.tipo == 'schema':
            return str(self.schema)
        elif self.tipo != 'total':
            resposta = {"layers": [], "root": {}}
            data = json.loads(TINYCUBES.get_response(self.msg))
            if data != '':
                resposta['root']['children'] = data
            if self.tipo == 'schema_date':
                resposta['layers'] = ['multi-target:Date']
            elif self.tipo == 'location':
                resposta['layers'] = ['anchor:location']
            elif self.tipo == 'category':
                resposta['layers'] = ['anchor:Primary_Type']
            elif self.tipo == 'date':
                resposta['layers'] = ["multi-target:Date"]
            resposta = json.dumps(resposta)
            return resposta
        else:
            resposta = '{"layers": [], "root": %s}'
            data = TINYCUBES.get_response(self.msg)
            if data != '':
                resposta = resposta % data
            else:
                resposta = resposta % '"val": 0'
            return resposta


class Servidor(servidor.BaseHTTPRequestHandler):

    def do_GET(self):
        request = self.requestline.split()[1]
        requisicao = Requisicao(request)
        self.send_response(200, "OK")
        self.send_header("Content-type", "application/jason")
        self.send_header("Access-Control-Allow-Origin", "http://localhost:8000")
        self.end_headers()
        self.wfile.write(str(requisicao).encode('utf-8'))


def abre_servidor():
    handler = Servidor
    try:
        with socketserver.TCPServer(('', 29512), handler) as http:
            http.serve_forever()

    except Exception as e:
        print(e)
        sys.exit(1)


def run_web_viewer():
    handler = servidor.SimpleHTTPRequestHandler
    with socketserver.TCPServer(('', 8000), handler) as httpd:
        print("Interface web na porta 8000")
        httpd.serve_forever()


def thread(n):
    if TINYCUBES.conectado:
        if n == 0:
            abre_servidor()
        elif n == 1:
            app = Interface()
            app.mainloop()
        elif n == 2:
            run_web_viewer()
        elif n == 3:
            sleep(0.5)
            system('start http://localhost:8000/web/#config')
    else:
        sleep(0.5)
        thread(n)


def main():
    for i in range(4):
        t = Thread(target=thread, args=(i,))
        t.start()


if __name__ == '__main__':
    main()

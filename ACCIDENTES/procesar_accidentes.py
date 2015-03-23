#Copyright 2015, Lesly Acuña, Renzo Massobrio

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.



# determine if a point is inside a given polygon or not
# Polygon is a list of (x,y) pairs.
import multiprocessing as mp
import string
import subprocess
import math
import os


################ PARAMETROS ##############
numero_de_hilos=4
ruta_archivo_accidentes="accidentes.csv"
ruta_nodos_barrios="nodos_barrios.csv"
##########################################

#Cola de resultados donde cada hilo coloca su resultado al finalizar
cola_resultados=mp.Queue()


#Cargo los poligonos que definen a los barrios 
barrios={}
f_barrios=open(ruta_nodos_barrios)
lines_barrios=f_barrios.readlines()
lines_barrios=lines_barrios[1:]
for line in lines_barrios:
    tokens=line.strip().split(",")
    if (tokens[2] in barrios):
        barrios[tokens[2]].append((float(tokens[1]), float(tokens[0])))
    else:
        barrios[tokens[2]]=[(float(tokens[1]), float(tokens[0]))]
f_barrios.close()


# Retorno TRUE si el punto (X,Y) se encuentra dentro del poligono poly, definido como una lista de pares (x,y)
def point_inside_polygon(x,y,poly):

    n = len(poly)
    inside =False

    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return inside


# Calcula los accidentes del archivo dado por ruta_archivo y coloca los resultados en la cola de resultados
def calcular_accidentes(ruta_archivo, cola_resultados):
    cant_afuera=0
    resultados={}
    for key in barrios.keys():
        resultados[key]=0

    f_accidentes=open(ruta_archivo)
    lines=f_accidentes.readlines()
    for line in lines:
        tokens=line.split(",")
        lat=float(tokens[1].strip())
        lng=float(tokens[0].strip())
        i=0
        while ((i<len(barrios.keys())) and (not point_inside_polygon(lat,lng,barrios[barrios.keys()[i]]))):
            i+=1

        if (i!=len(barrios.keys())):
            #El punto no esta en algun barrio, lo agrego
            resultados[barrios.keys()[i]]+=1
        else:
            cant_afuera+=1
    cola_resultados.put((resultados, cant_afuera))


# Divido el archivo de accidentes en tantas partes como cantidad de hilos
comando='wc -l '+ruta_archivo_accidentes
print comando
p = subprocess.Popen(comando, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    cantidad_lineas=int(line.strip().split(" ")[0])
retval = p.wait()

print "Cantidad de lineas archivo original: %d"%cantidad_lineas
cantidad_lineas_por_archivo=int(math.ceil(cantidad_lineas/float(numero_de_hilos)))
print "Cantidad de lineas por archivo: %d"%cantidad_lineas_por_archivo

comando='split -l '+str(cantidad_lineas_por_archivo)+ " " +ruta_archivo_accidentes+ " " + ruta_archivo_accidentes+"."
print comando
p = subprocess.Popen(comando, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    cantidad_lineas=line.strip().split(" ")[0]
retval = p.wait()


#Creo los diferentes hilos pasandoles el archivo correspondiente a cada uno
procesos = []
for i in range (0,numero_de_hilos):
    print "Creando hilo %s" %string.lowercase[i]
    ruta=ruta_archivo_accidentes+".a"+string.lowercase[i]
    procesos.append(mp.Process(target=calcular_accidentes, args=(ruta, cola_resultados)))

for p in procesos:
    p.start()

for p in procesos:
    p.join()


# Estructura para almacenar los resultados totales
cant_afuera_total=0
resultados_total={}
for key in barrios.keys():
    resultados_total[key]=0

# Junto los resultados de cada hilo en un total
for p in procesos:
    resultado=cola_resultados.get()
    for key in resultados_total.keys():
        resultados_total[key]+=resultado[0][key]
    cant_afuera_total+=resultado[1]


#Imprimo los resultados
print "RESULTADOS: (ID_BARRIO, NUMERO DE ACCIDENTES)"
print ""
print resultados_total
print "-----------------------------------"

total_accidentes=0
for key in resultados_total.keys():
    total_accidentes+=resultados_total[key]

print "TOTAL ACCIDENTES LOCALIZADOS: %d" %total_accidentes
print "TOTAL ACCIDENTES SIN ASIGNAR: %d"%cant_afuera_total

#Escribo el resultado a un archivo final
f = open('resultados_accidentes.csv','w')
for key in resultados_total.keys():
    f.write(key + "," + str(resultados_total[key])+ "\n")
f.close()
#Escribo el tipo de datos para qgis
f=open("resultados_accidentes.csvt", 'w')
f.write('"String","Integer"')
f.close()

print "Se creo el archivo resultados_accidentes.csv con los resultados para generar el mapa en QGIS"


# Elimino los archivos temporales generados

for i in range (0,numero_de_hilos):
    ruta=ruta_archivo_accidentes+".a"+string.lowercase[i]
    os.remove(ruta)

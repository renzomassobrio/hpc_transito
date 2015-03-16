# determine if a point is inside a given polygon or not
# Polygon is a list of (x,y) pairs.
import multiprocessing as mp
import string
import subprocess
import math
import os


################ PARAMETROS ##############
numero_de_hilos=4
ruta_archivo_multas="multas2007.csv"
ruta_archivo_vias="cruces_vias.csv"
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


#Cargo las vias
vias={}

#Cargo el archivo de vias
f_vias=open(ruta_archivo_vias)
lines_vias=f_vias.readlines()
lines_vias=lines_vias[1:]
for line in lines_vias:
  tokens=line.split(",")
  vias[(tokens[2].strip(), tokens[3].strip())]=(float(tokens[1].strip()), float(tokens[0].strip()))
f_vias.close()


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




def calcular_multas(ruta,cola_resultados):

	cant_vias_reconocidas=0
	cant_vias_erroneas=0
	cant_barrios_erroneos=0

	resultados_multas={}
	for key in barrios.keys():
		resultados_multas[key]=0

	f_multas=open(ruta)
	lines=f_multas.readlines()
	for line in lines:
	    tokens=line.split(",")
	    via1_multa=tokens[4].strip()
	    via2_multa=tokens[5].strip()
	    if (via1_multa,via2_multa) in vias:
	        lat=vias[(via1_multa,via2_multa)][0]
	        lng=vias[(via1_multa,via2_multa)][1]
	    if (via2_multa,via1_multa) in vias:
	        lat=vias[(via2_multa,via1_multa)][0]
	        lng=vias[(via2_multa,via1_multa)][1]

	    if (via1_multa,via2_multa) in vias or (via2_multa,via1_multa) in vias:
	        i=0
	        cant_vias_reconocidas+=1
	        while ((i<len(barrios.keys())) and (not point_inside_polygon(lat,lng,barrios[barrios.keys()[i]]))):
	            i+=1

	        if (i!=len(barrios.keys())):
	            #El punto no esta en algun barrio, lo agrego
	            resultados_multas[barrios.keys()[i]]+=1
	        else:
	            cant_barrios_erroneos+=1
	    else:
	        cant_vias_erroneas+=1

	cola_resultados.put((resultados_multas, cant_vias_reconocidas, cant_vias_erroneas, cant_barrios_erroneos))


# Divido el archivo de accidentes en tantas partes como cantidad de hilos
comando='wc -l '+ruta_archivo_multas
print comando
p = subprocess.Popen(comando, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    cantidad_lineas=int(line.strip().split(" ")[0])
retval = p.wait()

print "Cantidad de lineas archivo original: %d"%cantidad_lineas
cantidad_lineas_por_archivo=int(math.ceil(cantidad_lineas/float(numero_de_hilos)))
print "Cantidad de lineas por archivo: %d"%cantidad_lineas_por_archivo

comando='split -l '+str(cantidad_lineas_por_archivo)+ " " +ruta_archivo_multas+ " " + ruta_archivo_multas+"."
print comando
p = subprocess.Popen(comando, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
for line in p.stdout.readlines():
    cantidad_lineas=line.strip().split(" ")[0]
retval = p.wait()


#Creo los diferentes hilos pasandoles el archivo correspondiente a cada uno
procesos = []
for i in range (0,numero_de_hilos):
    print "Creando hilo %s" %string.lowercase[i]
    ruta=ruta_archivo_multas+".a"+string.lowercase[i]
    procesos.append(mp.Process(target=calcular_multas, args=(ruta, cola_resultados)))

for p in procesos:
    p.start()

for p in procesos:
    p.join()


#Junto los resultados

cant_vias_reconocidas_total=0
cant_vias_erroneas_total=0
cant_barrios_erroneos_total=0
resultados_multas_total={}
for key in barrios.keys():
	resultados_multas_total[key]=0


for p in procesos:
    resultado=cola_resultados.get()
    for key in resultados_multas_total.keys():
        resultados_multas_total[key]+=resultado[0][key]
    cant_vias_reconocidas_total+=resultado[1]
    cant_vias_erroneas_total+=resultado[2]
    cant_barrios_erroneos_total+=resultado[3]


#Imprimo los resultados
print "RESULTADOS: (ID_BARRIO, NUMERO DE MULTAS)"
print ""
print resultados_multas_total
print "-----------------------------------"

total_multas=0
for key in resultados_multas_total.keys():
    total_multas+=resultados_multas_total[key]

print "TOTAL MULTAS LOCALIZADAS: %d" %total_multas
print "TOTAL CRUCES LOCALIZADOS: %d "%cant_vias_reconocidas_total
print "TOTAL CRUCES ERRONEOS: %d"%cant_vias_erroneas_total
print "TOTAL PUNTOS SIN BARRIO: %d"%cant_barrios_erroneos_total

#Escribo el resultado a un archivo final
f = open('resultados_multas.csv','w')
for key in resultados_multas_total.keys():
    f.write(key + "," + str(resultados_multas_total[key])+ "\n")
f.close()
#Escribo el tipo de datos para qgis
f=open("resultados_multas.csvt", 'w')
f.write('"String","Integer"')
f.close()

print "Se creo el archivo resultados_multas.csv con los resultados para generar el mapa en QGIS"


# Elimino los archivos temporales generados

for i in range (0,numero_de_hilos):
    ruta=ruta_archivo_multas+".a"+string.lowercase[i]
    os.remove(ruta)
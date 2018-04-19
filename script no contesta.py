#importar pandas
import pandas as pd
import MySQLdb
from pandasql import sqldf
from datetime import datetime,timedelta
import random

fecha_ini='2018-04-19'
fecha_fin='2018-04-20'

corriente="in ('5554','5552')"
convenio="='5553'"
colas=[corriente,convenio]
nombre_colas=['CORRIENTE','CONVENIO']
comentarios=['','CAMPAÑA CONVENIO: ']
archivo_nuevo='corriente.xlsx'
writer = pd.ExcelWriter(archivo_nuevo, engine='xlsxwriter')

db = MySQLdb.connect(host="172.18.55.99",    # tu host, usualmente localhost
                     user="comandato",         # tu usuario
                     passwd="comandato123",  # tu password
                     db="call_center")        # el nombre de la base de datos
db2 = MySQLdb.connect(host="172.18.55.99",    # tu host, usualmente localhost
                     user="comandato",         # tu usuario
                     passwd="comandato123",  # tu password
                     db="SISTEMECUADOR_ATM")        # el nombre de la base de datos

cur2= db2.cursor()
cur= db2.cursor()
for cola,nombre_cola,comentario in zip(colas,nombre_colas,comentarios):
	consulta="""
		SELECT 
		c.id_campaign,cola.name
		FROM
		calls c,(SELECT id,name FROM campaign where queue """+cola+""") as cola
		WHERE c.start_time between '"""+fecha_ini+"""' and '"""+fecha_fin+"""' 
		and c.id_campaign=cola.id GROUP BY c.id_campaign;"""
	campañas=pd.read_sql(consulta, con=db)
	print(nombre_cola)
	print(campañas)
	id_camp=''
	for row in campañas.itertuples():
		id_camp=id_camp+','+str(row.id_campaign)
	id_camp=id_camp[1:]
	consulta_2="""SELECT
						valores.valor AS cedula,
						c.phone AS telefono,
						c.datetime_originate AS fecha
					FROM
						calls c 	LEFT JOIN
						
						(SELECT 
							call_attribute.id_call AS id_call,
							call_attribute.value AS valor
						FROM
							calls, call_attribute
							WHERE
						call_attribute.column_number = 2
						AND calls.id_campaign in ({0})
						AND calls.id = call_attribute.id_call) AS valores

						ON valores.id_call = c.id

					WHERE
					
					c.id_campaign in ({0})
					AND (c.status = 'Success'
					OR c.status = 'Failure'
					OR c.status = 'ShortCall'
					OR c.status = 'NoAnswer'
					OR c.status = 'Abandoned')
					AND c.datetime_originate between '{1}' and '{2}'
					ORDER BY uniqueid ASC;""".format(id_camp,fecha_ini,fecha_fin)
	resultado=pd.read_sql(consulta_2, con=db)
	
	resultado.to_excel(writer, sheet_name=nombre_cola)
	worksheet = writer.sheets[nombre_cola]

	i=0
	contador_ya_gestion=0
	contador_subidos=0
	for row2 in resultado.itertuples():
		telefono=str(row2.telefono)
		cedula=str(row2.cedula)
		fech_gest=str(row2.fecha)
		ob_fecha=datetime.strptime(fech_gest,'%Y-%m-%d %H:%M:%S')
		fech_prox=str(ob_fecha+ timedelta(days=3))
		usuario=str('SISTEMC'+str(random.randrange(1,28,1)).zfill(2))
		aleatorio= str(random.randrange(30, 70, 1))
		busc_querry="""SELECT CASA_COBRANZA 
						FROM SISTEMECUADOR_ATM.GESTION 
						where NRO_IDENTIFICACION_CLIENTE='{0}' 
						AND NUMERO_GESTION ='{1}' 
						and  FECHA_GESTION 
						between '{2} 07:00:00' and '{2} 20:30:00';""".format(cedula,telefono,fech_gest[:10])
		gestionado=False
		insert=False
		try:
			gestionado=cur2.execute(busc_querry)
			db2.commit()
			if gestionado:
				contador_ya_gestion=contador_ya_gestion+1
			else:
				ini_querry="""INSERT INTO `SISTEMECUADOR_ATM`.`GESTION` (`TIPO_GESTION`, 
											`NRO_IDENTIFICACION_CLIENTE`, `CASA_COBRANZA`,
											`COD_AGENTE`, `USUARIO_SAC`, `FECHA_GESTION`, 
											`ACCION_REALIZADA`, `RESPUESTA_OBTENIDA`, 
											`CONTACTO_GESTIONO`,`COMENTARIOS_GESTIONO`,
											`NUMERO_GESTION`, `FECHA_PROXIMA_GESTION`, 
											`TIEMPO_GESTION`, `CANAL_GESTION`) 
									VALUES ('1', '{0}', 'SISTEMA DE COBRO DEL ECUADOR',
											'D05554', '{1}', '{2}', 
											'HACER LLAMADA','NO CONTESTA', 'NO CONTACTO',
											'{3}NO CONTESTA','{4}',
											'{5}','{6}', 'GESTION');""".format(cedula,usuario,fech_gest,comentario,telefono,fech_prox,aleatorio)
				try:
					insert=cur.execute(ini_querry)
					db2.commit()
					contador_subidos=contador_subidos+1
				except:
					db2.rollback()
		except:
			db2.rollback()
		if i==800:
			break
		i+=1
	print("CANTIDAD DE CLIENTES QUE YA FUERON GESTIONADOS : "+str(contador_ya_gestion))
	print("CANTIDAD DE CLIENTES QUE FUERON SUBIDOS : "+str(contador_subidos))


#include "kernel.h"


void handler(int id) {

}

void assignHandler() {
	struct sigaction sa = {0};
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    sigaction(SIGUSR1, &sa, NULL);
}


int main() {

	inicializar_logs_y_configs();
	inicializar_kernel();


	//INICIA CLIENTE MEMORIA
	conectar_memoria();

	log_info(logger, "iniciando hilo INOTIFY");
	pthread_create(&tid_inotify, NULL, (void*)inotify_config, NULL);

	//HILO REINICIO_ESTADISTICAS
	pthread_create(&tid_estadisticas, NULL, (void*) reinicio_estadisticas, NULL);
	log_info(logger,"iniciando hilo estadisticas %d", tid_estadisticas);

	//HILO GOSSIPING
	pthread_create(&tid_gossiping, NULL, (void*) hilo_gossiping, NULL);
	log_info(logger,"iniciando hilo gossiping %d", tid_gossiping);

	//HILO DESCRIBE
	pthread_create(&tid_describe, NULL, (void*) hilo_describe, NULL);
	log_info(logger,"iniciando hilo describe %d", tid_describe);

	//INICIA CONSOLA
	pthread_t hilo_consola;
	pthread_create(&hilo_consola, NULL , (void*) consola, NULL);
	log_info(logger, "Kernel > Hilo creado de consola...");

	//INICIA HILOS DE EJECUCION
	crear_procesadores();

	pthread_join(tid_describe, NULL);
	pthread_join(hilo_consola, NULL);
	pthread_join(tid_estadisticas, NULL);
	pthread_join(tid_gossiping, NULL);
	pthread_join(tid_inotify, NULL);
	log_info(logger, "FIN hilo describe");
	log_info(logger, "FIN hilo inotify");
	log_info(logger, "FIN hilo consola");
	log_info(logger, "FIN hilo reinicio estadisticas");
	log_info(logger, "FIN hilo gossiping");

	liberar_kernel();

	return EXIT_SUCCESS;
}


void ejecutar_procesador(){

	t_PCB* pcb = NULL;
	char* linea = NULL;

	while(!exit_global){

		log_info(logger, "Esperando pcb...");

		sem_wait( &sem_cantidad_listos );
		pthread_mutex_lock(&sem_pcb);
			pcb = obtener_pcb_ejecutar();
		pthread_mutex_unlock(&sem_pcb);
		/*pthread_mutex_lock(&sem_ejecutar);*/
		/*pthread_mutex_unlock(&sem_ejecutar);*/

		if(pcb != NULL) {
		log_info(logger, "Se obtiene para ejecutar pcb id: %d", pcb->id);


			if( pcb->tipo_request == SIMPLE ){
				log_info(logger, "Se recibe a ejecucion request simple: %s" , pcb->request_comando  );
				ejecutar_linea( pcb->request_comando );

				pthread_mutex_lock(&sem_pcb);
					finalizar_pcb(pcb);
				pthread_mutex_unlock(&sem_pcb);
			}
			else
			{
				int k= 0;
				int res=0;

				char** split = string_split(pcb->request_comando, " ");
				log_info(logger, "Se recibe a ejecucion request compuesta archivo: %s" , pcb->request_comando  );
				char* path = strdup("../lql/");
				string_append(&path, split[1]);
				FILE* archivo = fopen( path , "r");

				if (archivo == NULL){

					log_error(logger, "No pude abrir la ruta:%s",split[1]);
					split_liberar(split);
					free(path);
					log_info(logger, "procedo a finalizar pcb %d",pcb->id);

					pthread_mutex_lock(&sem_pcb);
						finalizar_pcb(pcb);
					pthread_mutex_unlock(&sem_pcb);

				}else{

				apuntar_archivo(archivo, pcb->pc);
				split_liberar(split);
				free(path);

				while( (k < kernel_config.QUANTUM) && (!feof(archivo)) && res != -1 ){

					linea = obtener_linea(archivo);

					if(linea != NULL){

						log_info(logger, "la linea a ejecutar es: %s" , linea  );
						res = ejecutar_linea( linea );
						
						k++;
						pcb->pc++;
						free(linea);
						if(res == 0){
							log_info(logger, "la linea se ejecuto correctamente");
						}
					}
				}

				//si es fin de archivo o es un error de ejecucion finalizo el pcb
				if( feof(archivo)  ){
					log_info(logger, "Fin archivo pcb:%d",pcb->id);
					pthread_mutex_lock(&sem_pcb);
						finalizar_pcb(pcb);
					pthread_mutex_unlock(&sem_pcb);
				} 
				else if( res== -1 ){
					log_info(logger, "linea incorrecta pcb:%d",pcb->id);
					pthread_mutex_lock(&sem_pcb);
						finalizar_pcb(pcb);
					pthread_mutex_unlock(&sem_pcb);
				}	
				else if( k== kernel_config.QUANTUM ){
					pthread_mutex_lock(&sem_pcb);
						parar_por_quantum(pcb);
						log_info(logger, "Fin quantum pcb:%d",pcb->id);
						sem_post( &sem_cantidad_listos );
					pthread_mutex_unlock(&sem_pcb);
				}
				fclose(archivo);

				}
			}
		}
	}
	log_info(logger,"cerrando hilo");
	pthread_exit(0);
}

int ejecutar_linea( char *linea ){

	int res=0;
	t_tabla_consistencia *tabla=NULL;
	t_memoria_del_pool *memoria=NULL;

	char** parametros = string_split(linea, " ");

	if (es_string(linea,"\n")) return -1;
	//si es CREATE DESCRIBE DROP
	if (es_string(parametros[0],"CREATE") || es_string(parametros[0],"DESCRIBE") || es_string(parametros[0],"DROP") ) {


		if (es_string(parametros[0],"DROP") ) {

			//verifico que este en la metadata
			pthread_mutex_lock(&sem_tablas);
				tabla = obtener_tabla( parametros[1] );
			pthread_mutex_unlock(&sem_tablas);
			if( tabla == NULL ){

				log_info(logger, "No existe en la metadata del sistema la tabla:%s .Se aborta script", parametros[1] );
				res=-1;
				split_liberar(parametros);
				return res;
			}
		}
		if (es_string(parametros[0],"CREATE") ) {

			//verifico que este en la metadata
			pthread_mutex_lock(&sem_tablas);
				tabla = obtener_tabla( parametros[1] );
			pthread_mutex_unlock(&sem_tablas);
			if( tabla != NULL ){

				log_info(logger, "La tabla:%s ya se encuentra creada en el sistema. No es posible volver a crearla.Se aborta script ", parametros[1] );
				res=-1;
				split_liberar(parametros);
				return res;
			}
		}

		pthread_mutex_lock(&sem_memorias);
			memoria = obtener_memoria_random( l_memorias );
		pthread_mutex_unlock(&sem_memorias);

		if( memoria == NULL ) {
			log_info(logger, "Memoria para ejecutar no encontrada. Linea no ejecutada: %s" , linea );
			res=0;
			split_liberar(parametros);
			return res;
		}

		log_info(logger, "Memoria a ejecutar: %d", memoria->numero_memoria );
		res = ejecutar_linea_memoria( memoria , linea );
	}
	//es un insert o select
	else{

		pthread_mutex_lock(&sem_tablas);
			tabla = obtener_tabla( parametros[1] );
		pthread_mutex_unlock(&sem_tablas);
		if( tabla != NULL ){
			log_info(logger, "Tabla encontrada: %s Consistencia: %s", tabla->nombre_tabla,tabla->criterio_consistencia );

			pthread_mutex_lock(&sem_memorias);
				memoria = obtener_memoria_criterio( tabla, linea);
			pthread_mutex_unlock(&sem_memorias);

			if( memoria != NULL ){

				log_info(logger, "Memoria a ejecutar: %d", memoria->numero_memoria );
				res = ejecutar_linea_memoria( memoria , linea );
			}
			else{
				log_info(logger, "Memoria para ejecutar no encontrada  Linea no ejecutada: %s" , linea );
				res=0;
				split_liberar(parametros);
				return res;
			}
		}
		else{
			log_info(logger, "No se encuentra la tabla. Operacion: %s no ejecutada. Se aborta script", linea );
			res= -1;
			split_liberar(parametros);
			return res;
		}
	}

	split_liberar(parametros);
	return res;
}


t_tabla_consistencia *obtener_tabla( char* n_tabla ){

	t_tabla_consistencia *tabla=NULL;

	bool buscar_tabla( t_tabla_consistencia* tabla_it ){

		if( string_equals_ignore_case( n_tabla , tabla_it->nombre_tabla )  ) return true;
		return false;
	}

	if(!list_is_empty(l_tablas))
	{
		tabla = list_find(l_tablas , (void*)buscar_tabla);
	}

	return tabla;
}

t_memoria_del_pool *obtener_memoria_criterio( t_tabla_consistencia* tabla, char* linea){

	t_memoria_del_pool *memoria=NULL;

	if( string_equals_ignore_case( tabla->criterio_consistencia ,"SC" ) ){

		memoria = obtener_memoria_SC();
	}

	else if(string_equals_ignore_case( tabla->criterio_consistencia ,"EC") ){

		memoria = obtener_memoria_EC();
	}

	else if( string_equals_ignore_case( tabla->criterio_consistencia ,"SHC") ){

		memoria = obtener_memoria_SHC(linea);
	}

	return memoria;
}

t_memoria_del_pool *obtener_memoria_SC(){


	if( !list_is_empty( l_criterio_SC ) ) return list_get( l_criterio_SC , 0 );

	return NULL;
}

t_memoria_del_pool *obtener_memoria_EC(){

	t_memoria_del_pool* mem = NULL;
	if( !list_is_empty( l_criterio_EC)){
		int index = rand_num(list_size( l_criterio_EC));
		mem = list_get( l_criterio_EC, index);
	}
	return mem;
}

t_memoria_del_pool *obtener_memoria_SHC(char* linea){
	t_memoria_del_pool* mem = NULL;
	int index;
	char** split = string_split( linea, " ");
	log_info(logger, "%d", list_size(l_criterio_SHC));
	if( !list_is_empty( l_criterio_SHC )){
		if( string_equals_ignore_case(split[0], "INSERT") || string_equals_ignore_case(split[0], "SELECT") ){
			index = atoi(split[2]) % list_size( l_criterio_SHC);
			mem = list_get( l_criterio_SHC, index);
		} else{
			index = rand_num(list_size( l_criterio_SHC));
			mem = list_get( l_criterio_SHC, index);
		}
	}

	split_liberar(split);
	return mem;
}

int ejecutar_linea_memoria( t_memoria_del_pool* memoria , char* linea ){


	int socket;
	int res=0;
	int res_send = 0;
	int res_recv = 0;

/*	if(memoria->socket != -1){
		socket = memoria->socket;
	}
	else{
		socket = socket_connect_to_server(memoria->ip, memoria->puerto);
		log_info(logger, "El socket devuelto es: %d", socket);
		if( socket == -1  ){

			log_error(logger, "¡Error no se pudo conectar con MEMORIA:%d", memoria->numero_memoria );
			log_info(logger, "Se deshabilita memoria:%d", memoria->numero_memoria );
			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);

			return -1;
		}
		log_info(logger, "Se creo el socket cliente con MEMORIA de numero: %d en la memoria: %d", socket , memoria->numero_memoria);
		memoria->socket = socket;

	}
*/


	socket = socket_connect_to_server(memoria->ip, memoria->puerto);
		log_info(logger, "El socket devuelto es: %d", socket);
		if( socket == -1  ){

			log_error(logger, "¡Error no se pudo conectar con MEMORIA:%d", memoria->numero_memoria );
			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			log_info(logger, "Error de conexion con memoria:%d. Linea no ejecutada: %s" ,memoria->numero_memoria ,  linea );
			return 0;
		}
	log_info(logger, "Se creo el socket cliente con MEMORIA de numero: %d en la memoria: %d", socket , memoria->numero_memoria);

	char** split = string_split(linea, " ");

	if(es_string(split[0], "INSERT")){

		log_info(logger, "ALGORITMIA INSERT");
		int tiempo_ejecucion = clock();
		
		char* value = string_extract_substring(linea, "\"", "\"");
		remove_value(linea, value);
		split = string_split(linea, " ");
		
		linea_insert insert;
		insert.tabla = split[1];
		insert.key = (uint16_t) atoi(split[2]);
		insert.value = value;
		

		res_send = enviar_insert(insert, &socket);
		if( res_send == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			res=0;
			log_info( logger , "Falla send de enviar_insert. Se da de baja la memoria y se sale de la operacion." );
			log_info( logger , "Linea no ejecutada: %s", linea );
		}
		else{

			t_header paquete_recv;
			res_recv = recv(socket, &paquete_recv, sizeof(t_header), MSG_WAITALL);

			if( res_recv == -1 ){
				pthread_mutex_lock(&sem_memorias);
					desactivar_memoria(memoria);
				pthread_mutex_unlock(&sem_memorias);
				res=0;
				log_info( logger , "Falla recv de insert. Falla operacion: %s", linea );
			}
			else{
				if(paquete_recv.tipo_mensaje == EJECUCIONERROR ){
					log_info( logger , "Memoria informa falla de de operacion:%s .Se aborta script" , linea );
					res = -1;
				}
				else{
					log_info( logger , "operacion: %s realizada. Memoria: %d", linea , memoria->numero_memoria );
					memoria->cantidad_carga++;
					memoria->cantidad_insert++;
					memoria->tiempo_insert = (clock() - tiempo_ejecucion);
					operaciones_totales++;
				}
			}
		}
	}
	else if(es_string(split[0], "SELECT")){

		int tiempo_ejecucion = clock();

		linea_select select;
		select.tabla = split[1];
		select.key = (uint16_t) atoi(split[2]);

		res_send = enviar_select(select, &socket);
		if( res_send == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			res=0;
			log_info( logger , "Falla send de enviar_select. Se da de baja la memoria y se sale de la operacion." );
			log_info( logger , "Falla operacion: %s", linea );
		}
		else{

			t_header paquete_recv;
			res_recv = recv(socket, &paquete_recv, sizeof(t_header), MSG_WAITALL);
			if( res_recv == -1 ){
				pthread_mutex_lock(&sem_memorias);
					desactivar_memoria(memoria);
				pthread_mutex_unlock(&sem_memorias);
				res=0;
				log_info( logger , "Falla recv de SELECT. Falla operacion: %s", linea );
			}
			else{
				if(paquete_recv.tipo_mensaje == EJECUCIONERROR ) {
					res = 0;
					log_info( logger , "operacion: %s no existe value en memoria: %d", linea  , memoria->numero_memoria);
				}
				else{

					char* buffer = malloc(paquete_recv.payload_size);
					res_recv = recv(socket, buffer, paquete_recv.payload_size, MSG_WAITALL);
					if( res_recv == -1 ){
						pthread_mutex_lock(&sem_memorias);
							desactivar_memoria(memoria);
						pthread_mutex_unlock(&sem_memorias);
						free(buffer);
						res=0;
						log_info( logger , "Falla recv de SELECT. Falla operacion: %s", linea );
					}
					else{
						linea_response_select response_select;
						deserializar_response_select(buffer, &response_select);
						log_info( logger, "SELECT a %s KEY %d es: %s\n", select.tabla, select.key, response_select.value);
						log_info( logger , "operacion: %s value: %s memoria: %d", linea  , response_select.value , memoria->numero_memoria);

						free(buffer);
						free(response_select.value);

						memoria->cantidad_carga++;
						memoria->cantidad_select++;
						memoria->tiempo_select = (clock() - tiempo_ejecucion);
						operaciones_totales++;
					}

				}

			}
		}

	}
	else if(es_string(split[0], "CREATE")){

		linea_create create;
		create.tabla = split[1];
		create.tipo_consistencia = split[2];
		create.nro_particiones = (u_int8_t) atoi(split[3]);
		create.tiempo_compactacion = (u_int32_t) atoi(split[4]);

		res_send = enviar_create(create, &socket);
		if( res_send == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			res=0;
			log_info( logger , "Falla send de enviar_create. Se da de baja la memoria y se sale de la operacion." );
			log_info( logger , "Falla operacion: %s", linea );
		}
		else{

			t_header paquete_recv;
			res_recv = recv(socket, &paquete_recv, sizeof(t_header), MSG_WAITALL);
			if( res_recv == -1 ){
				pthread_mutex_lock(&sem_memorias);
					desactivar_memoria(memoria);
				pthread_mutex_unlock(&sem_memorias);
				res=0;
				log_info( logger , "Falla recv de CREATE. Falla operacion: %s", linea );
			}
			else{
				if(paquete_recv.tipo_mensaje == EJECUCIONERROR ){
					//TODO:verificar si se aborta script o no
					log_info( logger , "Falla CREATE en memoria. Falla operacion:%s " , linea );
					res = 0;
				}
				else{
					//TODO: verificar si existe la tabla
					t_tabla_consistencia *tabla = malloc(sizeof(t_tabla_consistencia));
					tabla->criterio_consistencia = strdup(split[2]);
					tabla->nombre_tabla = strdup(split[1]);
					pthread_mutex_lock(&sem_tablas);
						list_add(l_tablas, tabla);
					pthread_mutex_unlock(&sem_tablas);
					operaciones_totales++;
					log_info(logger,"tabla %s creada, criterio: %s memoria: %d", split[1],split[2] , memoria->numero_memoria);
				}
			}
		}


	}
	else if(es_string(split[0], "DROP")){

		char* tabla = split[1];

		res_send = enviar_drop(&socket, tabla);
		if( res_send == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			res=0;
			log_info( logger , "Falla send de enviar_drop. Se da de baja la memoria y se sale de la operacion." );
			log_info( logger , "Falla operacion: %s", linea );
		}
		else{
			t_header paquete_recv;
			res_recv =recv(socket, &paquete_recv, sizeof(t_header), MSG_WAITALL);
			if( res_recv == -1 ){
				pthread_mutex_lock(&sem_memorias);
					desactivar_memoria(memoria);
				pthread_mutex_unlock(&sem_memorias);
				res=0;
				log_info( logger , "Falla recv de DROP. Falla operacion: %s", linea );
			}
			else{
				if(paquete_recv.tipo_mensaje == EJECUCIONERROR ) {
					//TODO: verificar si aca se aborta o no script
					log_info( logger , "Falla operacion: %s con memoria", linea );
					res =0;
				}
				else{
					log_info(logger,"se hizo drop de la tabla %s con memoria:%d",split[1] , memoria->numero_memoria);
					pthread_mutex_lock(&sem_tablas);
						quitar_tabla_lista( split[1]);
					pthread_mutex_unlock(&sem_tablas);
					operaciones_totales++;
				}
			}
		}
	}
	else if(es_string(split[0], "DESCRIBE")){

		if(split[1] == NULL){

			log_info(logger,"Ejecuto DESCRIBE general");
			res = describe(memoria);
			if( res == -1 ) {
				//TODO: verificar si falla describe general se aborta script o no.
				log_info(logger, "Falla describe con memoria:%d",memoria->numero_memoria);

			}
			else {
				operaciones_totales++;
				log_info(logger, "Se realiza describe exitosamente con memoria:%d",memoria->numero_memoria);
			}
			res=0;
		}
		else{

			log_info(logger,"Ejecuto DESCRIBE de tabla %s" , split[1] );
			res_send = enviar_describe_especial(&socket, split[1]);
			if( res_send == -1 ){

				pthread_mutex_lock(&sem_memorias);
					desactivar_memoria(memoria);
				pthread_mutex_unlock(&sem_memorias);
				res=0;
				log_info( logger , "Falla send de enviar_descibe de tabla. Se da de baja la memoria y se sale de la operacion." );
				log_info( logger , "Falla operacion: %s", linea );
			}
			else{

				t_header paquete_recv;
				res_recv = recv(socket, &paquete_recv, sizeof(t_header), MSG_WAITALL);
				if( res_recv == -1 ){
					pthread_mutex_lock(&sem_memorias);
						desactivar_memoria(memoria);
					pthread_mutex_unlock(&sem_memorias);
					res=0;
					log_info( logger , "Falla recv de DESCRIBE. Falla operacion: %s", linea );
				}
				else{
					if(paquete_recv.tipo_mensaje == EJECUCIONERROR ){
						log_info( logger , "Falla DESCRIBE con memoria. Falla operacion: %s", linea );
						res = 0;
					}
					else{

						char* buffer = malloc(paquete_recv.payload_size);
						res_recv = recv(socket, buffer, paquete_recv.payload_size, MSG_WAITALL);
						if( res_recv == -1 ){
							pthread_mutex_lock(&sem_memorias);
								desactivar_memoria(memoria);
							pthread_mutex_unlock(&sem_memorias);
							res=0;
							log_info( logger , "Falla recv de DESCRIBE. Falla operacion: %s", linea );
						}
						else{
							t_list *lista_tablas = deserializar_describe(buffer);
							pthread_mutex_lock(&sem_tablas);
								list_iterate( lista_tablas , (void*)agregar_tabla_describe );
								log_info(logger,"Se termino de ejecutar DESCRIBE con memoria: %d",memoria->numero_memoria);
							pthread_mutex_unlock(&sem_tablas);
							list_destroy_and_destroy_elements( lista_tablas , (void*)free_tabla_describe);
							free(buffer);
							operaciones_totales++;
						}
					}
				}
			}
		}
	}
	else{
		log_error(logger,"comando no reconocido. Se aborta script");
		res = -1;
	}
	
	
	if( socket != -1  ){
		close(socket);
	}
	retardo();
	split_liberar(split);
	return res;
}



void agregar_tabla_describe( linea_create* tabla_describe ){

	bool tabla_encontrada( t_tabla_consistencia *tabla ){

		if( string_equals_ignore_case(tabla->nombre_tabla , tabla_describe->tabla ) ) return true;
		return false;
	}

	//si no la encuentra la agrego a la lista
	if( list_find( l_tablas , (void*)tabla_encontrada ) == NULL ){

		t_tabla_consistencia *tabla_nueva = malloc( sizeof(t_tabla_consistencia ) );
		tabla_nueva->nombre_tabla = strdup( tabla_describe->tabla );
		tabla_nueva->criterio_consistencia = strdup( tabla_describe->tipo_consistencia );
		tabla_nueva->nro_particiones = tabla_describe->nro_particiones ;
		tabla_nueva->tiempo_compactacion = tabla_describe->tiempo_compactacion;

		list_add(l_tablas , tabla_nueva );
		log_info(logger, "se agrega la tabla: %s",  tabla_nueva->nombre_tabla );
	}
	else{
		//log_info(logger, "ya se encuentra en el sistema la tabla: %s",  tabla_describe->tabla );

	}

}

t_PCB* obtener_pcb_ejecutar(){

	/*
	//si lista vacia se queda loopeando esperando que entre alguno
	while( (list_is_empty( l_pcb_listos )) && (!exit_global) ){

	}

	if(exit_global) return NULL;
	*/

	t_PCB *pcb = NULL;
	pcb = list_remove( l_pcb_listos , 0 );
	list_add( l_pcb_ejecutando , pcb  );
	log_info(logger, "se agrega a ejecucion pcb id %d",  pcb->id );
	log_info(logger, "nuevo tamanio de la lista de listos %d", list_size( l_pcb_listos ));

	return pcb;
}

void finalizar_pcb(t_PCB* pcb){

	bool buscar_pcb( t_PCB* pcb_it ){

		if(  pcb_it->id == pcb->id  ) return true;
		return false;
	}

	//quito de la lista pcb y lo agrego a finalizados
	list_remove_by_condition(l_pcb_ejecutando,(void*)buscar_pcb);
	log_info(logger, "nuevo tamanio de lista ejecutando: %d", list_size( l_pcb_ejecutando ));
	list_add( l_pcb_finalizados , pcb );
	log_info(logger, "tamanio lista finalizados: %d", list_size( l_pcb_finalizados ));

}

void inicializar_kernel(){

	id_pcbs = 0;
	operaciones_totales=0;
	exit_global = 0;

	pthread_mutex_init(&sem_ejecutar, NULL);
	pthread_mutex_init(&sem_memorias, NULL);
	pthread_mutex_init(&sem_tablas, NULL);
	pthread_mutex_init(&sem_pcb, NULL);
	sem_init(&sem_cantidad_listos, 0, 0);

	//INIT lista criterios
	l_criterio_SHC = list_create();
	l_criterio_SC = list_create();
	l_criterio_EC = list_create();

	//INIT lista de estados
	l_pcb_nuevos = list_create();
	l_pcb_listos = list_create();
	l_pcb_ejecutando = list_create();
	l_pcb_finalizados = list_create();

	l_memorias = list_create();

	l_procesadores = list_create();

	l_tablas = list_create();

}

void conectar_memoria(){

	socket_memoria = socket_connect_to_server(kernel_config.IP_MEMORIA, kernel_config.PUERTO_MEMORIA);
	if( socket_memoria == -1  ){

		log_error(logger, "¡Error no se pudo conectar con MEMORIA");
		exit(EXIT_FAILURE);
	}
	log_info(logger, "Se creo el socket cliente con MEMORIA de numero: %d", socket_memoria);
	t_header buffer;
	buffer.emisor=KERNEL;
	buffer.tipo_mensaje =CONEXION;
	buffer.payload_size = 0;
	send(socket_memoria, &buffer, sizeof( buffer ) , MSG_NOSIGNAL);
	int numero_memoria;
	recv(socket_memoria , &numero_memoria, sizeof(int), MSG_WAITALL);

	t_memoria_del_pool* memoria_original = malloc( sizeof( t_memoria_del_pool ) );
	memoria_original->ip = strdup(kernel_config.IP_MEMORIA);
	memoria_original->puerto = strdup(kernel_config.PUERTO_MEMORIA);
	memoria_original->activa=0; //no esta asociada a ningun criteiro no esta activa
	memoria_original->numero_memoria=numero_memoria;
	memoria_original->socket = socket_memoria;
	memoria_original->cantidad_carga = 0;
	memoria_original->cantidad_insert=0;
	memoria_original->cantidad_select=0;
	memoria_original->tiempo_insert=0;
	memoria_original->tiempo_select=0;

	pthread_mutex_lock(&sem_memorias);
		list_add(l_memorias , memoria_original );
		log_info(logger, "Se agrega memoria de archivo de configuracion con numero:%d" , numero_memoria);
	pthread_mutex_unlock(&sem_memorias);

	//hago gossiping con la memoria principal.
	gossiping( memoria_original );
	describe( memoria_original );
}

void crear_procesadores(){

	pthread_t hilo_ejecucion;

	for(int i=0; i<kernel_config.MULTIPROCESAMIENTO; i++){

		pthread_create(&hilo_ejecucion, NULL , (void*) ejecutar_procesador, NULL);
		log_info(logger, "Hilo de ejecucion creado");

		pthread_t* valor = malloc(sizeof(pthread_t));
		*valor = hilo_ejecucion;

		list_add(l_procesadores, valor);
		pthread_detach(hilo_ejecucion);
	}
}

char* obtener_linea(FILE* archivo){

	char leido;
	fread(&leido, sizeof(char),1,archivo);
	char* linea = string_from_format("%c", leido);
	while (!feof(archivo) && fread(&leido, sizeof(char),1,archivo) && leido != '\n'){
		string_append_with_format(&linea, "%c", leido);
	}

	return linea;
}

void apuntar_archivo(FILE* archivo, int pc){
	char leido;
	for(int i=0; i<pc; i++){
	while (!feof(archivo) && fread(&leido, sizeof(char),1,archivo) && leido != '\n'){}
	}

}


void parar_por_quantum(t_PCB* pcb){

	bool buscar_pcb( t_PCB* pcb_it ){

		if(  pcb_it->id == pcb->id  ) return true;
		return false;
	}
	list_remove_by_condition(l_pcb_ejecutando,(void*)buscar_pcb);
	log_info(logger, "nuevo tamanio de lista ejecutando: %d", list_size( l_pcb_ejecutando ));
	list_add( l_pcb_listos , pcb );
	log_info(logger, "tamanio lista de listos: %d", list_size( l_pcb_listos ));
}

int enviar_insert(linea_insert linea, void* sock){

	int socket = *(int*)sock;
	int tamanio;
	int res=0;
	char* buffer = serializar_insert( linea, &tamanio);

	t_header paquete;
	paquete.emisor = KERNEL;
	paquete.tipo_mensaje = INSERT;
	paquete.payload_size = tamanio;

	res = send(socket, &paquete, sizeof(t_header), MSG_NOSIGNAL);
	if(res != -1){
		res = send(socket, buffer, tamanio, MSG_NOSIGNAL);
	}

	free(buffer);

	return res;
}

int enviar_select(linea_select linea, void* sock){

	int socket = *(int*)sock;
	int tamanio;
	int res=0;
	char* buffer = serializar_select(linea, &tamanio);

	t_header paquete;
	paquete.emisor = KERNEL;
	paquete.tipo_mensaje = SELECT;
	paquete.payload_size = tamanio;

	res = send(socket, &paquete, sizeof(t_header), MSG_NOSIGNAL);
	if(res != -1){
		res = send(socket, buffer, tamanio, MSG_NOSIGNAL);
	}

	free(buffer);
	return res;
}

int enviar_create(linea_create linea, void* sock){

	int socket = *(int*)sock;
	int tamanio;
	int res=0;
	char* buffer = serializar_create(linea, &tamanio);

	t_header paquete;
	paquete.emisor = KERNEL;
	paquete.tipo_mensaje = CREATE;
	paquete.payload_size = tamanio;

	res = send(socket, &paquete, sizeof(t_header), MSG_NOSIGNAL);
	if(res != -1){
		res = send(socket, buffer, tamanio, MSG_NOSIGNAL);
	}

	free(buffer);
	return res;
}

int enviar_describe_general(void* sock){

	int socket = *(int*)sock;
	int res=0;
	t_header paquete;
	paquete.emisor = KERNEL;
	paquete.tipo_mensaje = DESCRIBE;
	paquete.payload_size = 0;

	res =send(socket, &paquete, sizeof(t_header), MSG_NOSIGNAL);

	return res;
}

int enviar_describe_especial(void* sock, char* tabla){

	int socket = *(int*)sock;
	int tamanio;
	int res=0;
	char* buffer = serializar_string(tabla, &tamanio);

	t_header paquete;
	paquete.emisor = KERNEL;
	paquete.tipo_mensaje = DESCRIBE;
	paquete.payload_size = tamanio;
	res = send(socket, &paquete, sizeof(t_header), MSG_NOSIGNAL);
	if( res != -1 ){
		res = send(socket, buffer, tamanio, MSG_NOSIGNAL);
	}

	free(buffer);
	return res;
}

int enviar_drop(void* sock,char* tabla){

	int socket = *(int*)sock;
	int tamanio;
	char* buffer = serializar_string(tabla, &tamanio);
	int res=0;

	t_header paquete;
	paquete.emisor = KERNEL;
	paquete.tipo_mensaje = DROP;
	paquete.payload_size = tamanio;
	res=send(socket, &paquete, sizeof(t_header), MSG_NOSIGNAL);
	if(res != -1){
		res=send(socket, buffer, tamanio, MSG_NOSIGNAL);
	}

	free(buffer);
	return res;
}

void reinicio_estadisticas(){

	struct timespec ts;

	//cada 30 segundos
	ts.tv_sec = 30000 / 1000;
	ts.tv_nsec = (30000 % 1000) * 1000000;

	assignHandler();

	while(!exit_global){

		nanosleep(&ts, NULL);
		log_info(logger, "iniciando reincio estadisticas");

		list_iterate(l_memorias,(void*)reiniciar_memorias);
	}

	pthread_exit(0);
}

void reiniciar_memorias(t_memoria_del_pool* memoria)
{
	log_info(logger,"reiniciando memoria: %d",memoria->numero_memoria);
	memoria->cantidad_insert = 0;
	memoria->cantidad_select = 0;
	memoria->tiempo_insert = 0;
	memoria->tiempo_select = 0;
}



void hilo_gossiping(){

	struct timespec ts;
	ts.tv_sec = kernel_config.GOSSIPING_REFRESH / 1000;
	ts.tv_nsec = (kernel_config.GOSSIPING_REFRESH % 1000) * 1000000;

	assignHandler();

	while(!exit_global){

		nanosleep(&ts, NULL);
		log_info(logger, "iniciando hilo gossiping");

		if( !list_is_empty(l_memorias ) ){

			pthread_mutex_lock(&sem_memorias);
				t_list* memorias_activas = l_memorias;
			pthread_mutex_unlock(&sem_memorias);
			if( list_is_empty( memorias_activas ) ){
				log_info(logger, "No se encuentra memorias disponibles para realizar gossiping");
			}
			else{

				list_iterate( memorias_activas , (void*)gossiping );
			}

			//list_destroy( memorias_activas );
			log_info(logger, "Fin hilo gossiping");
		}
	}

	pthread_exit(0);
}


void gossiping( t_memoria_del_pool *memoria ){

	int res = 0;

	log_info(logger, "Se comienza gossiping con memoria: %d" , memoria->numero_memoria);
	/*
	if( memoria->socket == -1 ){

		int socketmemoria = socket_connect_to_server(memoria->ip,  memoria->puerto );
		log_info(logger, "el socket es :%d" ,socketmemoria);

		if( socketmemoria == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria( memoria );
				log_info(logger, "no se pudo conectar con memoria:%d. se rechaza gossiping:%d" , memoria->numero_memoria);
			pthread_mutex_unlock(&sem_memorias);

			return;
		}
		log_info(logger, "Se establece conexion con memoria: %d: socket: %d" , memoria->numero_memoria , memoria->socket);
		memoria->socket=socketmemoria;
	}
*/
	
	int socketmemoria = socket_connect_to_server(memoria->ip,  memoria->puerto );
		log_info(logger, "el socket es :%d" ,socketmemoria);

		if( socketmemoria == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria( memoria );
				log_info(logger, "no se pudo conectar con memoria:%d. se rechaza gossiping:%d" , memoria->numero_memoria);
			pthread_mutex_unlock(&sem_memorias);

			return;
		}
	
	t_header buffer;
	buffer.emisor=KERNEL;
	buffer.tipo_mensaje =  GOSSIPING;
	buffer.payload_size = 0;
	log_info(logger, "socket de memoria es:%d",memoria->socket);
	res = send(socketmemoria, &buffer, sizeof( buffer ) , MSG_NOSIGNAL);
	log_info(logger, "La respuesta de memoria conexion es:%d",res);
	if( res == -1 ){

		pthread_mutex_lock(&sem_memorias);
			desactivar_memoria( memoria );
			log_info(logger, "Fallo el envio de gossiuping a memoria:%d" , memoria->numero_memoria );
		pthread_mutex_unlock(&sem_memorias);
		return;
	}
	/*log_info(logger, "Ejecuto el send correctamente para gossiping" );
	t_header paquete_recv;
	recv(memoria->socket, &paquete_recv, sizeof(t_header), MSG_WAITALL);
	if(paquete_recv.tipo_mensaje == EJECUCIONERROR ) {
		log_info(logger, "Fallo la ejecucion de gossiping en memoria. Se desestima la operacion");
		return;
	}
*/
	t_header header_tabla;
	res = recv(socketmemoria , &header_tabla, sizeof(t_header), MSG_WAITALL);
	if( res == -1 ){

		pthread_mutex_lock(&sem_memorias);
			desactivar_memoria( memoria );
			log_info(logger, "Fallo el recv de gossiuping a memoria:%d" , memoria->numero_memoria );
		pthread_mutex_unlock(&sem_memorias);
		return;
	}
	char *buffer_tabla = malloc( header_tabla.payload_size);
	res = recv(socketmemoria , buffer_tabla, header_tabla.payload_size , MSG_WAITALL);
	if( res == -1 ){

		pthread_mutex_lock(&sem_memorias);
			desactivar_memoria( memoria );
			log_info(logger, "Fallo el recv de gossiuping a memoria:%d" , memoria->numero_memoria );
		pthread_mutex_unlock(&sem_memorias);
		free( buffer_tabla );
		return;
	}

	t_list *memorias_seed = deserializar_memorias(buffer_tabla);

	//recorro lista de gossiping y agrego las nuevas
	pthread_mutex_lock(&sem_memorias);
		list_iterate( memorias_seed , (void*)agregar_memoria_gossip );
	pthread_mutex_unlock(&sem_memorias);
	
	close(socketmemoria);
	free( buffer_tabla );
	liberar_memorias_gossiping(memorias_seed);

	log_info(logger, "Se termina proceso de gossiping con memoria:%d" , memoria->numero_memoria);
	return;
}


void agregar_memoria_gossip( pmemoria *memoria ){

	bool memoria_encontrada( t_memoria_del_pool *memoria_pool ){

		if( memoria_pool->numero_memoria == memoria->numero_memoria ) return true;
		return false;
	}

	//si no la encuentra la agrego a la lista
	if( list_find( l_memorias , (void*)memoria_encontrada ) == NULL ){

		t_memoria_del_pool* memoria_nueva = malloc( sizeof( t_memoria_del_pool ) );
		memoria_nueva->activa=0; //desactivada ya que no esta asociada a ningun criterio
		memoria_nueva->numero_memoria=memoria->numero_memoria;
		memoria_nueva->ip = strdup(memoria->ip);
		memoria_nueva->puerto = strdup(memoria->puerto);
		memoria_nueva->cantidad_carga=0;
		memoria_nueva->cantidad_insert=0;
		memoria_nueva->cantidad_select=0;
		memoria_nueva->socket=-1;
		list_add(l_memorias , memoria_nueva );
		log_info(logger, "se agrega al pool la memoria: %d",  memoria_nueva->numero_memoria );
	}
	else{
		log_info(logger, "ya se encuentra en el pool la memoria: %d",  memoria->numero_memoria );
	}


}


void hilo_describe(){

	struct timespec ts;

	assignHandler();

	while(!exit_global){

		ts.tv_sec = kernel_config.METADATA_REFRESH / 1000;
		ts.tv_nsec = (kernel_config.METADATA_REFRESH % 1000) * 1000000;

		nanosleep(&ts, NULL);
		log_info(logger, "iniciando describe en hilo");

		if( !list_is_empty(l_memorias ) ){

			pthread_mutex_lock(&sem_memorias);
				t_memoria_del_pool *memoria = obtener_memoria_random( l_memorias );
			pthread_mutex_unlock(&sem_memorias);

			if( memoria== NULL ){
				log_info(logger, "No se encuentra memoria disponible para realizar describe en hilo");
			}
			else{

				log_info(logger, "Memoria elegida para describe en hilo es: %d",memoria->numero_memoria);
				int res = describe( memoria );

				if( res== -1 ){

					log_info(logger, "Falla describe con memoria: %d",memoria->numero_memoria);
				}
				else{
					log_info(logger, "Se realiza describe exitosamente con memoria:%d",memoria->numero_memoria);
				}

			}
		}
		log_info(logger, "Fin describe en hilo");
	}

	pthread_exit(0);
}

int describe( t_memoria_del_pool *memoria ){

	/*
	if( memoria->socket == -1 ){

		int socketmemoria = socket_connect_to_server(memoria->ip,  memoria->puerto );
		log_info(logger, "%d" ,socketmemoria);

		if( socketmemoria == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			log_info(logger, "no se pudo conectar con memoria. se rechaza describe");
			return -1;
		}
		log_info(logger, "Se establece conexion con memoria: %d: socket: %d" , memoria->numero_memoria , memoria->socket);
		memoria->socket=socketmemoria;
	}*/

	int socketmemoria = socket_connect_to_server(memoria->ip,  memoria->puerto );
		log_info(logger, "%d" ,socketmemoria);

		if( socketmemoria == -1 ){

			pthread_mutex_lock(&sem_memorias);
				desactivar_memoria(memoria);
			pthread_mutex_unlock(&sem_memorias);
			log_info(logger, "no se pudo conectar con memoria. se rechaza describe");
			return -1;
		}
	
	log_info(logger,"comienza DESCRIBE con memoria:%d" , memoria->numero_memoria);
	int res_send = enviar_describe_general(&socketmemoria);
	if( res_send == -1 ){

		pthread_mutex_lock(&sem_memorias);
			desactivar_memoria(memoria);
		pthread_mutex_unlock(&sem_memorias);
		log_info(logger, "Fallo el envio de describe general. Se rechaza describe");
		return -1;
	}

	t_header paquete_recv;
	res_send = recv(socketmemoria, &paquete_recv, sizeof(t_header), MSG_WAITALL);
	if( res_send == -1 ){

		pthread_mutex_lock(&sem_memorias);
			desactivar_memoria(memoria);
		pthread_mutex_unlock(&sem_memorias);
		log_info(logger, "Fallo el recv de describe general. Se rechaza describe");
		return -1;
	}
	if(paquete_recv.tipo_mensaje == EJECUCIONERROR ) {
		log_info(logger, "Fallo la ejecucion de describe en memoria/lfs. Se rechaza describe");
		return -1;
	}else{

	/*t_header paquete;
	recv(memoria->socket , &paquete, sizeof(t_header), MSG_WAITALL);*/
	char* buffer = malloc(paquete_recv.payload_size);
	res_send = recv(socketmemoria , buffer, paquete_recv.payload_size, MSG_WAITALL);
	if( res_send == -1 ){

		pthread_mutex_lock(&sem_memorias);
			desactivar_memoria(memoria);
		pthread_mutex_unlock(&sem_memorias);
		log_info(logger, "Fallo el recv de describe general. Se rechaza describe");
		return -1;
	}
	t_list *lista_tablas = deserializar_describe(buffer);

	pthread_mutex_lock(&sem_tablas);
		list_iterate( lista_tablas , (void*)agregar_tabla_describe );
	pthread_mutex_unlock(&sem_tablas);
	list_destroy_and_destroy_elements( lista_tablas , (void*)free_tabla_describe);
	free(buffer);
	close(socketmemoria);
	}
	return 0;
}


void quitar_tabla_lista( char* tabla ){

	bool es_tabla( t_tabla_consistencia *tabla_it ){

		if( string_equals_ignore_case( tabla , tabla_it->nombre_tabla )) return true;
		return false;
	}

	t_tabla_consistencia *tabla_encontrada = list_remove_by_condition( l_tablas , (void*)es_tabla );

	if( tabla_encontrada != NULL ){
		log_info(logger,"se quita de la metadata la tabla %s",tabla_encontrada->nombre_tabla);
		free_tabla( tabla_encontrada );
	}
	else{
		log_info(logger,"La tabla: %s a la que se hizo drop no estaba en la metadata " ,tabla);
	}
}


void desactivar_memoria(t_memoria_del_pool *memoria){


	bool memoria_encontrada( t_memoria_del_pool *memoria_pool ){

		if( memoria_pool->numero_memoria == memoria->numero_memoria ) return true;
		return false;
	}

	t_memoria_del_pool *memoria_desactivada = NULL;

	memoria_desactivada = list_remove_by_condition(l_criterio_SHC, (void*)memoria_encontrada);
	if( memoria_desactivada != NULL  ){

		log_info(logger,"Se quita del criterio SHC la memoria: %d " ,memoria_desactivada->numero_memoria);
		memoria_desactivada=NULL;
		log_info(logger,"Se envia journal a las memorias del criterio SHC");
		enviar_journal_lista_memorias( l_criterio_SHC );
	}

	memoria_desactivada = list_remove_by_condition(l_criterio_SC, (void*)memoria_encontrada);
	if( memoria_desactivada != NULL  ){

			log_info(logger,"Se quita del criterio SC la memoria: %d " ,memoria_desactivada->numero_memoria);
			memoria_desactivada=NULL;
	}

	memoria_desactivada = list_remove_by_condition(l_criterio_EC, (void*)memoria_encontrada);
	if( memoria_desactivada != NULL  ){

			log_info(logger,"Se quita del criterio EC la memoria: %d " ,memoria_desactivada->numero_memoria);
			memoria_desactivada=NULL;
	}

	memoria->activa = false;
	memoria->socket = -1;
}


void inotify_config(){

	char buffer[BUF_LEN];
	int file_descriptor = inotify_init();

	assignHandler();

	if (file_descriptor < 0) {
		perror("inotify_init");
	}
	log_info(logger, "inicia inotify");

	int watch_descriptor = inotify_add_watch(file_descriptor, CONFIG_FOLDER, IN_MODIFY | IN_CREATE | IN_CLOSE_WRITE);

	while(!exit_global){

		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0) {
			perror("read");
		}

		int offset = 0;

		while (offset < length) {

			struct inotify_event *event = (struct inotify_event *) &buffer[offset];

			if (event->len) {

				if (string_contains(event->name, CONFIG_FILE)){
					log_info(logger, "Config File changed");
					config = config_create("config/kernel.cfg");
					kernel_config.SLEEP_EJECUCION = config_get_int_value(config, "SLEEP_EJECUCION");
					kernel_config.METADATA_REFRESH = config_get_int_value(config, "METADATA_REFRESH");
					kernel_config.QUANTUM = config_get_int_value(config,"QUANTUM");
					config_destroy(config);
					loggear_configs();
				}
			}
			offset += sizeof (struct inotify_event) + event->len;
		}

	}

	inotify_rm_watch(file_descriptor, watch_descriptor);
	close(file_descriptor);


	pthread_exit(0);
}


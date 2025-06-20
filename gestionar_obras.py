# gestionar_obras.py
import pandas as pd
import numpy as np
import peewee
from abc import ABC, abstractmethod
import datetime

# Importamos los modelos definidos en modelo_orm.py
from modelo_orm import db, Etapa, TipoObra, AreaResponsable, Comuna, Barrio, \
                       TipoContratacion, Empresa, Financiamiento, Obra, MODELOS


class GestionarObra(ABC):

    _dataframe_obras = None

    @classmethod 
    def extraer_datos(cls, path_csv='observatorio-de-obras-urbanas.csv', delimiter=';', encoding='utf-8'):
        """
        a. Extrae los datos del archivo CSV utilizando pandas y los retorna como un DataFrame.
           Incluye manejo de excepciones para archivo no encontrado y codificación.
        """
        try:
            print(f"Intentando extraer datos de: {path_csv} con codificación {encoding}")
            cls._dataframe_obras = pd.read_csv(path_csv, sep=delimiter, encoding=encoding)
            print("Datos extraídos correctamente.")
            return cls._dataframe_obras # aca llamamos al csv para extraer los datos
    
        except FileNotFoundError:
            print(f"ERROR: El archivo '{path_csv}' no fue encontrado. Asegúrate de que esté en la misma carpeta que el script.")
            return None # Retorna None si no se encuentra el archivo
        
        except UnicodeDecodeError as ude:
            print(f"ERROR de codificación al leer el CSV: {ude}")
            return None  # ude es simplificado unicode decode error, que ocurre si el encoding no es correcto
        
        except pd.errors.EmptyDataError:
            print(f"ERROR: El archivo '{path_csv}' está vacío.")
            return None # Retorna None si el archivo está vacío
        
        except Exception as e:
            print(f"ERROR inesperado al extraer datos: {e}") # e simplificado de exception, captura cualquier otro error inesperado
            return None 

    @classmethod
    def conectar_db(cls):
        try:
            db.connect()
            print("Conexión a la base de datos 'obras_urbanas.db' establecida correctamente.")
            return True
        except peewee.OperationalError as e:
            print(f"ERROR: No se pudo conectar a la base de datos. {e}")
            return False
        except Exception as e:
            print(f"ERROR inesperado al conectar la base de datos: {e}")
            return False

    @classmethod # <--- ENSURE THIS DECORATOR IS PRESENT
    def mapear_orm(cls):
        """
        c. Crea la estructura de la base de datos (tablas y relaciones)
           utilizando el método create_tables(list) de Peewee.
           Incluye manejo de excepciones.
        """
        try:
            db.create_tables(MODELOS)
            print("Tablas de la base de datos creadas/verificadas correctamente.")
            return True
        except peewee.OperationalError as e:
            print(f"ERROR: No se pudieron crear las tablas de la base de datos. {e}")
            return False
        except Exception as e:
            print(f"ERROR inesperado al mapear ORM: {e}")
            return False

    @classmethod # <--- ENSURE THIS DECORATOR IS PRESENT
    def limpiar_datos(cls, df):
        """
        d. Realiza la limpieza de datos nulos y no accesibles del DataFrame.
           Normaliza nombres de columnas y maneja tipos de datos.
           Retorna el DataFrame limpio.
        """
        if df is None:
            print("No hay DataFrame para limpiar. Ejecuta 'extraer_datos' primero.")
            return None

        df_limpio = df.copy()

        print("Iniciando limpieza y normalización de datos...")

        df_limpio.columns = df_limpio.columns.str.lower().str.replace('-', '_').str.strip()
        print("Nombres de columnas normalizados.")

        df_limpio = df_limpio.replace(r'^\s*$', np.nan, regex=True)

        if 'monto_contrato' in df_limpio.columns:
            df_limpio['monto_contrato'] = df_limpio['monto_contrato'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df_limpio['monto_contrato'] = pd.to_numeric(df_limpio['monto_contrato'], errors='coerce')
        print("Columna 'monto_contrato' procesada.")

        date_columns = ['fecha_inicio', 'fecha_fin_inicial']
        for col in date_columns:
            if col in df_limpio.columns:
                df_limpio[col] = pd.to_datetime(df_limpio[col], errors='coerce', dayfirst=True)
        print("Columnas de fechas procesadas.")

        coord_columns = ['lat', 'lng']
        for col in coord_columns:
            if col in df_limpio.columns:
                 df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce')
        print("Columna de coordenadas procesadas.")

        if 'porcentaje_avance' in df_limpio.columns:
            df_limpio['porcentaje_avance'] = pd.to_numeric(df_limpio['porcentaje_avance'], errors='coerce')
        print("Columna 'porcentaje_avance' procesada.")

        bool_columns = ['destacada', 'ba_elige']
        for col in bool_columns:
            if col in df_limpio.columns:
                df_limpio[col] = df_limpio[col].astype(str).str.lower().map({'si': True, 'no': False, '1': True, '0': False}).fillna(False)
        print("Columnas booleanas procesadas.")

        int_columns = ['comuna', 'plazo_meses', 'mano_obra', 'licitacion_anio']
        for col in int_columns:
            if col in df_limpio.columns:
                df_limpio[col] = pd.to_numeric(df_limpio[col], errors='coerce').astype('Int64')
        print("Columnas enteras procesadas.")

        print("Limpieza y normalización de datos completada.")
        return df_limpio

    @classmethod
    def cargar_datos(cls, df_limpio):
        """
        e. Persiste los datos de las obras (ya transformados y limpios)
           que contiene el objeto DataFrame en la base de datos relacional SQLite.
           Utiliza el método de clase Model.create() en cada una de las clases del modelo ORM.
           Implementa manejo de excepciones y transacciones para una carga eficiente.
        """
        if df_limpio is None:
            print("No hay DataFrame limpio para cargar. Ejecuta 'limpiar_datos' primero.")
            return False

        print("Iniciando carga de datos a la base de datos...")
        try:
            with db.atomic():
                for index, row in df_limpio.iterrows():
                    try:
                        # --- MODIFICACIONES AQUÍ PARA NORMALIZAR STRINGS ---
                        # Función auxiliar para limpiar strings, eliminando los caracteres problemáticos
                        def clean_string(text):
                            if pd.isna(text):
                                return None
                            return str(text).strip()
                            text = str(text)
                            text = text.replace('', 'ñ') # Reemplaza el carácter común de error por 'ñ'
                            text = text.replace('Pblico', 'Público') # Reemplaza el string completo si es un patrón
                            text = text.replace('Hbitat', 'Hábitat')
                            text = text.replace('Secretara', 'Secretaría')
                            text = text.replace('ulica', 'úlica') # Para Hidraúlica
                            text = text.replace('Espacio Pblico y Vial', 'Espacio Público y Vial')
                            return text.strip()

                        etapa_nombre = clean_string(row['etapa'])
                        tipo_obra_nombre = clean_string(row['tipo'])
                        area_responsable_nombre = clean_string(row['area_responsable'])
                        barrio_nombre = clean_string(row['barrio'])
                        tipo_contratacion_nombre = clean_string(row['contratacion_tipo'])
                        empresa_nombre = clean_string(row['licitacion_oferta_empresa'])
                        financiamiento_nombre = clean_string(row['financiamiento'])

                        etapa_obj, _ = Etapa.get_or_create(nombre=etapa_nombre)
                        tipo_obra_obj, _ = TipoObra.get_or_create(nombre=tipo_obra_nombre)
                        area_responsable_obj, _ = AreaResponsable.get_or_create(nombre=area_responsable_nombre)
                        # --- FIN MODIFICACIONES ---

                        comuna_val = row['comuna'] if pd.notna(row['comuna']) else None
                        comuna_obj = None
                        if comuna_val is not None:
                            comuna_obj, _ = Comuna.get_or_create(numero=comuna_val)

                        barrio_obj = None
                        if barrio_nombre and comuna_obj: # Usa barrio_nombre limpio
                            barrio_obj, _ = Barrio.get_or_create(nombre=barrio_nombre, comuna=comuna_obj) # Usa barrio_nombre limpio
                        elif barrio_nombre and not comuna_obj: # Si hay barrio pero no comuna válida
                             barrio_obj = None

                        tipo_contratacion_obj = None
                        if tipo_contratacion_nombre: # Usa el nombre limpio
                            tipo_contratacion_obj, _ = TipoContratacion.get_or_create(nombre=tipo_contratacion_nombre)

                        empresa_obj = None
                        if empresa_nombre: # Usa el nombre limpio
                            cuit_val = row['cuit_contratista'] if pd.notna(row['cuit_contratista']) else None
                            # Para Empresa, el get_or_create es un poco más delicado si hay cuit duplicados con nombres diferentes.
                            # Si cuit es UNIQUE, el get_or_create con cuit es el primario.
                            # Si el cuit es nulo, get_or_create se basará solo en el nombre.
                            # Por ahora, mantenemos la lógica actual, que ya maneja los UNIQUE constraint failures.
                            empresa_obj, _ = Empresa.get_or_create(nombre=empresa_nombre, cuit=cuit_val)


                        financiamiento_obj = None
                        if financiamiento_nombre: # Usa el nombre limpio
                            financiamiento_obj, _ = Financiamiento.get_or_create(nombre=financiamiento_nombre)


                        attrs = {
                            'entorno': row['entorno'] if pd.notna(row['entorno']) else None,
                            'nombre': row['nombre'] if pd.notna(row['nombre']) else 'Obra sin nombre',
                            'descripcion': row['descripcion'] if pd.notna(row['descripcion']) else None,
                            'monto_contrato': row['monto_contrato'] if pd.notna(row['monto_contrato']) else None,
                            'direccion': row['direccion'] if pd.notna(row['direccion']) else None,
                            'lat': row['lat'] if pd.notna(row['lat']) else None,
                            'lng': row['lng'] if pd.notna(row['lng']) else None,
                            'fecha_inicio': row['fecha_inicio'].date() if pd.notna(row['fecha_inicio']) else None,
                            'fecha_fin_inicial': row['fecha_fin_inicial'].date() if pd.notna(row['fecha_fin_inicial']) else None,
                            'plazo_meses': row['plazo_meses'] if pd.notna(row['plazo_meses']) else None,
                            'porcentaje_avance': row['porcentaje_avance'] if pd.notna(row['porcentaje_avance']) else None,
                            'imagen_1': row['imagen_1'] if pd.notna(row['imagen_1']) else None,
                            'imagen_2': row['imagen_2'] if pd.notna(row['imagen_2']) else None,
                            'imagen_3': row['imagen_3'] if pd.notna(row['imagen_3']) else None,
                            'imagen_4': row['imagen_4'] if pd.notna(row['imagen_4']) else None,
                            'licitacion_anio': row['licitacion_anio'] if pd.notna(row['licitacion_anio']) else None,
                            'nro_contratacion': row['nro_contratacion'] if pd.notna(row['nro_contratacion']) else None,
                            'beneficiarios': row['beneficiarios'] if pd.notna(row['beneficiarios']) else None,
                            'mano_obra': row['mano_obra'] if pd.notna(row['mano_obra']) else None,
                            'compromiso': row['compromiso'] if pd.notna(row['compromiso']) else None,
                            'destacada': row['destacada'] if pd.notna(row['destacada']) else False,
                            'ba_elige': row['ba_elige'] if pd.notna(row['ba_elige']) else False,
                            'link_interno': row['link_interno'] if pd.notna(row['link_interno']) else None,
                            'pliego_descarga': row['pliego_descarga'] if pd.notna(row['pliego_descarga']) else None,
                            'expediente_numero': row['expediente_numero'] if pd.notna(row['expediente_numero']) else None,
                            'estudio_ambiental_descarga': row['estudio_ambiental_descarga'] if pd.notna(row['estudio_ambiental_descarga']) else None,
                            'etapa': etapa_obj,
                            'tipo_obra': tipo_obra_obj,
                            'area_responsable': area_responsable_obj,
                            'comuna': comuna_obj,
                            'barrio': barrio_obj,
                            'contratacion_tipo': tipo_contratacion_obj,
                            'licitacion_oferta_empresa': empresa_obj,
                            'financiamiento': financiamiento_obj
                        }
                        Obra.create(**attrs)

                    except peewee.IntegrityError as ie:
                        print(f"Advertencia: Error de integridad al cargar fila {index}: {ie}. Saltando esta fila.")
                    except Exception as ex:
                        print(f"Error inesperado al procesar la fila {index}: {ex}. Datos: {row.to_dict()}")

            print(f"Datos cargados correctamente. Total de obras procesadas: {len(df_limpio)}.")
            return True
        except peewee.OperationalError as e:
            print(f"ERROR en la operación de base de datos durante la carga: {e}")
            return False
        except Exception as e:
            print(f"ERROR inesperado durante la carga de datos: {e}")
            return False
        finally:
            pass # db.close() no es necesario aquí si la conexión se maneja globalmente en main.py


        """
        Método auxiliar para solicitar y validar un valor de Foreign Key
        que debe existir en la BD.
        Si el valor no existe, muestra las opciones disponibles.
        """
        while True:
            valor_ingresado = input(f"{prompt_text}: ").strip()
            if not valor_ingresado:
                print("Este campo no puede estar vacío.")
                continue
            try:
                if field_name == 'numero':
                    instance = model_class.get(getattr(model_class, field_name) == int(valor_ingresado))
                else:
                    instance = model_class.get(getattr(model_class, field_name) == valor_ingresado)
                return instance
            except (peewee.DoesNotExist, ValueError):
                print(f"'{valor_ingresado}' no existe en la tabla de {model_class.__name__}.")
                
                print(f"Valores existentes para {model_class.__name__}:")
                try:
                    existing_values = model_class.select(getattr(model_class, field_name)).distinct().order_by(getattr(model_class, field_name))
                    
                    if existing_values.count() > 0:
                        for val in existing_values:
                            print(f"- {getattr(val, field_name)}")
                    else:
                        print(f"  (No hay valores cargados para {model_class.__name__} aún. Asegúrate de cargar el CSV primero.)")
                    
                except peewee.OperationalError as e:
                    print(f"  Error de base de datos al intentar leer valores existentes: {e}")
                except Exception as e:
                    print(f"  Error inesperado al listar valores: {e}")

                print("Por favor, ingrese un valor existente.")
            except peewee.OperationalError as e:
                print(f"Error de conexión a la base de datos al buscar {model_class.__name__}: {e}. Asegúrese de que la DB esté activa.")
                return None
            except Exception as e:
                print(f"Error al buscar en {model_class.__name__}: {e}")

    @classmethod # <--- ENSURE THIS DECORATOR IS PRESENT
    def nueva_obra(cls):
        """
        f. Crea nuevas instancias de Obra, solicitando valores por teclado.
           Valida que las Foreign Keys existan o solicita reingreso.
           Persiste la nueva instancia usando Model.save() y retorna la instancia.
        """
        print("\n--- Creación de Nueva Obra ---")
        # No necesitamos db.connect() aquí, main.py ya la abrió
        try:
            nombre = input("Nombre de la Obra: ").strip()
            while not nombre:
                nombre = input("El nombre de la Obra no puede estar vacío. Ingrese nuevamente: ").strip()

            descripcion = input("Descripción (opcional): ").strip() or None
            direccion = input("Dirección (opcional): ").strip() or None
            monto_contrato_str = input("Monto del Contrato (ej: 123456.78, opcional): ").strip() or None
            monto_contrato = float(monto_contrato_str) if monto_contrato_str else None

            tipo_obra_obj = cls._solicitar_fk_existente(TipoObra, 'nombre', "Tipo de Obra")
            if tipo_obra_obj is None: return None

            area_responsable_obj = cls._solicitar_fk_existente(AreaResponsable, 'nombre', "Área Responsable")
            if area_responsable_obj is None: return None

            comuna_obj = None
            while True:
                comuna_input = input("Comuna (número entero, opcional): ").strip()
                if not comuna_input:
                    break
                try:
                    comuna_num = int(comuna_input)
                    comuna_obj, created = Comuna.get_or_create(numero=comuna_num)
                    if created:
                        print(f"Comuna {comuna_num} creada (si no existía).")
                    break
                except ValueError:
                    print("Comuna debe ser un número entero válido.")
                except Exception as e:
                    print(f"Error al procesar comuna: {e}")

            barrio_obj = None
            if comuna_obj:
                barrio_nombre = input("Barrio (opcional, debe existir en la comuna seleccionada o se ignorará si no se encuentra): ").strip()
                if barrio_nombre:
                    try:
                        barrio_obj = Barrio.get(Barrio.nombre == barrio_nombre, Barrio.comuna == comuna_obj)
                    except peewee.DoesNotExist:
                        print(f"El barrio '{barrio_nombre}' no existe para la comuna {comuna_obj.numero}. Será ignorado.")
                        barrio_obj = None
                    except Exception as e:
                        print(f"Error al buscar barrio: {e}")
            else:
                print("No se puede especificar un barrio sin una comuna válida.")

            nueva_obra_obj = Obra(
                nombre=nombre,
                descripcion=descripcion,
                direccion=direccion,
                monto_contrato=monto_contrato,
                tipo_obra=tipo_obra_obj,
                area_responsable=area_responsable_obj,
                comuna=comuna_obj,
                barrio=barrio_obj,
            )
            nueva_obra_obj.save()
            print(f"Obra '{nombre}' creada con éxito. ID: {nueva_obra_obj.id}")

            nueva_obra_obj.nuevo_proyecto()

            return nueva_obra_obj

        except peewee.OperationalError as e:
            print(f"ERROR de conexión a la base de datos al crear nueva obra: {e}. Asegúrese de que la DB esté activa.")
            return None
        except Exception as e:
            print(f"ERROR inesperado al crear nueva obra: {e}")
            return None


    @classmethod # <--- ENSURE THIS DECORATOR IS PRESENT
    def obtener_indicadores(cls):
        """
        g. Obtiene y muestra por consola la información de las obras existentes.
           Utiliza sentencias ORM para las consultas.
        """
        print("\n--- Indicadores de Obras Urbanas ---")
        # No necesitamos db.connect() aquí, main.py ya la abrió
        try:
            print("\n1. Áreas Responsables:")
            areas = AreaResponsable.select().order_by(AreaResponsable.nombre)
            for area in areas:
                print(f"- {area.nombre}")

            print("\n2. Tipos de Obra:")
            tipos = TipoObra.select().order_by(TipoObra.nombre)
            for tipo in tipos:
                print(f"- {tipo.nombre}")

            print("\n3. Cantidad de obras por etapa:")
            obras_por_etapa = (Obra.select(Obra.etapa.nombre, peewee.fn.COUNT(Obra.id).alias('cantidad'))
                                   .join(Etapa)
                                   .group_by(Obra.etapa.nombre)
                                   .order_by(peewee.fn.COUNT(Obra.id).desc()))
            for res in obras_por_etapa:
                print(f"- {res.etapa.nombre}: {res.cantidad} obras")

            print("\n4. Obras y Monto de Inversión por Tipo de Obra:")
            inversion_por_tipo = (Obra.select(Obra.tipo_obra.nombre,
                                              peewee.fn.COUNT(Obra.id).alias('cantidad'),
                                              peewee.fn.SUM(Obra.monto_contrato).alias('monto_total'))
                                      .join(TipoObra)
                                      .group_by(Obra.tipo_obra.nombre)
                                      .order_by(peewee.fn.COUNT(Obra.id).desc()))
            for res in inversion_por_tipo:
                monto_total = f"${res.monto_total:,.2f}" if res.monto_total else "N/A"
                print(f"- {res.tipo_obra.nombre}: {res.cantidad} obras, Inversión total: {monto_total}")

            print("\n5. Barrios en Comunas 1, 2 y 3:")
            barrios_en_comunas = (Barrio.select(Barrio.nombre, Barrio.comuna.numero)
                                        .join(Comuna)
                                        .where(Comuna.numero.in_([1, 2, 3]))
                                        .order_by(Comuna.numero, Barrio.nombre))
            if not barrios_en_comunas.exists():
                print("   No se encontraron barrios para las comunas 1, 2 y 3. Asegúrate de que las comunas y barrios estén cargados.")
            else:
                current_comuna = None
                for barrio in barrios_en_comunas:
                    if barrio.comuna.numero != current_comuna:
                        print(f"\n  Comuna {barrio.comuna.numero}:")
                        current_comuna = barrio.comuna.numero
                    print(f"   - {barrio.nombre}")

            print("\n6. Cantidad de obras finalizadas en <= 24 meses:")
            obras_finalizadas_en_plazo = (Obra.select()
                                            .join(Etapa)
                                            .where(
                                                (Etapa.nombre == "Finalizada") &
                                                (Obra.plazo_meses <= 24)
                                            ).count())
            print(f"- {obras_finalizadas_en_plazo} obras finalizadas en 24 meses o menos.")

            print("\n7. Monto total de inversión:")
            monto_total_general = Obra.select(peewee.fn.SUM(Obra.monto_contrato)).scalar()
            monto_total_general_formato = f"${monto_total_general:,.2f}" if monto_total_general else "N/A"
            print(f"- El monto total de inversión de todas las obras es: {monto_total_general_formato}")

        except peewee.OperationalError as e:
            print(f"ERROR en la operación de base de datos al obtener indicadores: {e}")
        except Exception as e:
            print(f"ERROR inesperado al obtener indicadores: {e}")
        finally:
            pass
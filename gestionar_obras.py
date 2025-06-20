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

    @classmethod
    def mapear_orm(cls): #crea tablas y relaciones en la base de datos
        try:
            db.create_tables(MODELOS) # en modelos esta los nombres de las tablas
            print("Tablas de la base de datos creadas/verificadas correctamente.")
            return True
        except peewee.OperationalError as e:
            print(f"ERROR: No se pudieron crear las tablas de la base de datos. {e}")
            return False
        except Exception as e:
            print(f"ERROR inesperado al mapear ORM: {e}")
            return False
    

    @classmethod
    def limpiar_datos(cls, df):
        if df is None:
            print("No hay DataFrame para limpiar. Ejecuta 'extraer_datos' primero.")
            return None

        df_limpio = df.copy()
        df_limpio.columns = df_limpio.columns.str.lower().str.replace('-', '_').str.strip()
        df_limpio = df_limpio.applymap(lambda x: np.nan if isinstance(x, str) and x.strip() == '' else x)
        # Elimina filas completamente vacías
        df_limpio = df_limpio.dropna(how='all')
        print("Datos limpiados y normalizados.")
        return df_limpio
    


@classmethod
def cargar_datos(cls, df_limpio):
    if df_limpio is None or df_limpio.empty:
        print("No hay DataFrame limpio para cargar. Ejecuta 'limpiar_datos' primero.")
        return False

    print("Iniciando carga de datos a la base de datos...")
    try:
        with db.atomic():
            for index, row in df_limpio.iterrows():
                print("\nRegistro a cargar:")
                print(row)
                confirm = input("¿Desea cargar este registro? (s/n): ").strip().lower()
                if confirm != 's':
                    print("Registro omitido.")
                    continue

                try:
                    etapa_obj, _ = Etapa.get_or_create(nombre=str(row['etapa']).strip() if pd.notna(row['etapa']) else None)
                    tipo_obra_obj, _ = TipoObra.get_or_create(nombre=str(row['tipo']).strip() if pd.notna(row['tipo']) else None)
                    area_responsable_obj, _ = AreaResponsable.get_or_create(nombre=str(row['area_responsable']).strip() if pd.notna(row['area_responsable']) else None)
                    comuna_obj = None
                    if pd.notna(row['comuna']):
                        comuna_obj, _ = Comuna.get_or_create(numero=int(row['comuna']))
                    barrio_obj = None
                    if pd.notna(row['barrio']) and comuna_obj:
                        barrio_obj, _ = Barrio.get_or_create(nombre=str(row['barrio']).strip(), comuna=comuna_obj)

                    Obra.create(
                        nombre=row['nombre'] if pd.notna(row['nombre']) else "Obra sin nombre",
                        descripcion=row['descripcion'] if pd.notna(row['descripcion']) else None,
                        monto_contrato=row['monto_contrato'] if pd.notna(row['monto_contrato']) else None,
                        tipo_obra=tipo_obra_obj,
                        area_responsable=area_responsable_obj,
                        etapa=etapa_obj,
                        comuna=comuna_obj,
                        barrio=barrio_obj
                        # Agrega aquí otros campos si los necesitas
                    )
                    print("Registro cargado correctamente.")
                except Exception as e:
                    print(f"Error al cargar el registro {index}: {e}")
        print("Carga finalizada.")
        return True
    except Exception as e:
        print(f"ERROR inesperado durante la carga de datos: {e}")
        return False

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
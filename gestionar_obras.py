# gestionar_obras.py
import pandas as pd
import numpy as np
import peewee
from peewee import OperationalError, fn
from abc import ABC, abstractmethod
import datetime

# Importamos los modelos definidos en modelo_orm2.py
from modelo_orm2 import db, Etapa, TipoObra, AreaResponsable, Comuna, Barrio, TipoContratacion, Empresa, Financiamiento, Obra, MODELOS


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
        except OperationalError as e:
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
        except OperationalError as e:
            print(f"ERROR: No se pudieron crear las tablas de la base de datos. {e}")
            return False
        except Exception as e:
            print(f"ERROR inesperado al mapear ORM: {e}")
            return False
    

    
    @classmethod
    def limpiar_datos(cls, df):
        # Elimina filas completamente vacías
        df_limpio = df.dropna(how='all')

        # Elimina filas donde los campos clave están vacíos o NaN
        campos_clave = ['nombre', 'etapa', 'tipo', 'area_responsable']
        df_limpio = df_limpio.dropna(subset=campos_clave, how='any')

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
                    campos_importantes = ['nombre', 'etapa', 'tipo', 'area_responsable']
                    # Si falta algún campo importante, no lo cargues
                    if any(pd.isna(row[campo]) for campo in campos_importantes if campo in row):
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
                        )
                        print(f"Registro {index} cargado correctamente.")
                    except Exception as e:
                        print(f"Error al cargar el registro {index}: {e}")
            print("Carga finalizada.")
            return True
        except Exception as e:
            print(f"ERROR inesperado durante la carga de datos: {e}")
            return False
        finally:
            if not db.is_closed():
                db.close()
                print("Conexión a la base de datos cerrada.")


    @classmethod
    def nueva_obra(cls):
        print("\n--- Crear Nueva Obra ---")
        try:
            nombre = input("Nombre de la obra: ").strip()
            descripcion = input("Descripción: ").strip()
            direccion = input("Dirección: ").strip()
            monto_contrato = input("Monto del Contrato (ej: 123456.78, opcional): ").replace("$", "").replace(",", ".").strip()
            monto_contrato = float(monto_contrato) if monto_contrato else None

            tipo_obra = cls._solicitar_fk_existente(TipoObra, "nombre", "Tipo de Obra")
            area_responsable = cls._solicitar_fk_existente(AreaResponsable, "nombre", "Área Responsable")
            comuna = cls._solicitar_fk_existente(Comuna, "numero", "Comuna")
            barrio = cls._solicitar_fk_existente(Barrio, "nombre", "Barrio")

            if not (tipo_obra and area_responsable and comuna and barrio):
                print("No se pudo crear la obra por falta de datos obligatorios.")
                return None

            obra = Obra.create(
                nombre=nombre,
                descripcion=descripcion,
                direccion=direccion,
                monto_contrato=monto_contrato,
                tipo_obra=tipo_obra,
                area_responsable=area_responsable,
                comuna=comuna,
                barrio=barrio,
                etapa=None
            )
            obra.nuevo_proyecto()
            print(f"Obra '{obra.nombre}' creada correctamente.")
            return obra
        except Exception as e:
            print(f"ERROR inesperado al crear nueva obra: {e}")
            return None

    @classmethod
    def obtener_indicadores(cls):
        """
        g. Obtiene y muestra por consola la información de las obras existentes.
        Utiliza sentencias ORM para las consultas.
        """
        print("\n--- Indicadores de Obras Urbanas ---")
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
            obras_por_etapa = (Obra.select(Obra.etapa.nombre, fn.COUNT(Obra.id).alias('cantidad'))
                                    .join(Etapa)
                                    .group_by(Obra.etapa.nombre)
                                    .order_by(fn.COUNT(Obra.id).desc()))
            for res in obras_por_etapa:
                print(f"- {res.etapa.nombre}: {res.cantidad} obras")

            print("\n4. Obras y Monto de Inversión por Tipo de Obra:")
            inversion_por_tipo = (Obra.select(Obra.tipo_obra.nombre,
                                            fn.COUNT(Obra.id).alias('cantidad'),
                                            fn.SUM(Obra.monto_contrato).alias('monto_total'))
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
            monto_total_general = Obra.select(fn.SUM(Obra.monto_contrato)).scalar()
            monto_total_general_formato = f"${monto_total_general:,.2f}" if monto_total_general else "N/A"
            print(f"- El monto total de inversión de todas las obras es: {monto_total_general_formato}")

        except OperationalError as e:
            print(f"ERROR en la operación de base de datos al obtener indicadores: {e}")
        except Exception as e:
            print(f"ERROR inesperado al obtener indicadores: {e}")

    @classmethod
    def _solicitar_fk_existente(cls, Modelo, campo, texto):
        opciones = [getattr(obj, campo) for obj in Modelo.select()]
        if not opciones:
            print(f"No hay opciones cargadas para {texto}.")
            return None
        while True:
            print(f"Opciones disponibles para {texto}:")
            for opcion in opciones:
                print(f"- {opcion}")
            valor = input(f"Ingrese {texto}: ").strip()
            try:
                return Modelo.get(getattr(Modelo, campo) == valor)
            except Modelo.DoesNotExist:
                print(f"{texto} '{valor}' no existe. Intente nuevamente.\n")
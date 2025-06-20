# modelo_orm.py
import peewee

# 1. Configuración de la base de datos
# Creamos una instancia de la base de datos SQLite
# Se creará un archivo llamado 'obras_urbanas.db' en la misma carpeta del proyecto
db = peewee.SqliteDatabase('obras_urbanas.db')

# 2. Clase BaseModel: Hereda de peewee.Model
# Define la conexión a la base de datos para todos los modelos.
class BaseModel(peewee.Model):
    class Meta:
        database = db # Esta es la base de datos que usará Peewee para todos los modelos

# 3. Modelos para Tablas Relacionadas (Clases de Apoyo / Tablas de Dimensión)
# Estas tablas almacenarán valores únicos de categorías para evitar duplicados en la tabla principal
# y permitir relaciones (Foreign Keys).

class Etapa(BaseModel):
    nombre = peewee.CharField(unique=True) # "Proyecto", "Contratación", "En ejecución", "Finalizada", "Rescindida"

    class Meta:
        db_table = 'etapas' # Nombre de la tabla en la DB

class TipoObra(BaseModel):
    nombre = peewee.CharField(unique=True) # "Arquitectura", "Vial", "Hidráulica", etc.

    class Meta:
        db_table = 'tipos_obra'

class AreaResponsable(BaseModel):
    nombre = peewee.CharField(unique=True) # "Jefatura de Gabinete", "Ministerio de Espacio Público e Higiene Urbana", etc.

    class Meta:
        db_table = 'areas_responsables'

class Comuna(BaseModel):
    numero = peewee.IntegerField(unique=True) # Ej: 1, 2, 3, ..., 15

    class Meta:
        db_table = 'comunas'

class Barrio(BaseModel):
    nombre = peewee.CharField(unique=True)
    comuna = peewee.ForeignKeyField(Comuna, backref='barrios') # Relación con Comuna

    class Meta:
        db_table = 'barrios'

class TipoContratacion(BaseModel):
    nombre = peewee.CharField(unique=True) # "Licitación Pública", "Contratación Directa", etc.

    class Meta:
        db_table = 'tipos_contratacion'

class Empresa(BaseModel):
    nombre = peewee.CharField(unique=True) # Nombre de la empresa contratista
    cuit = peewee.CharField(unique=True, null=True) # CUIT de la empresa, puede ser nulo

    class Meta:
        db_table = 'empresas'

class Financiamiento(BaseModel):
    nombre = peewee.CharField(unique=True) # "GCBA", "Nacional", "Internacional", etc.

    class Meta:
        db_table = 'financiamientos'

# 4. Modelo Principal: Obra
# Representa una obra urbana y se relaciona con las tablas anteriores.

class Obra(BaseModel):
    # Campos que mapean directamente del CSV (ajustando tipos de datos)
    entorno = peewee.CharField(null=True) # Puede ser "Obra de Gobierno" o similar
    nombre = peewee.CharField()
    descripcion = peewee.TextField(null=True) # Campo de texto más largo
    monto_contrato = peewee.DecimalField(max_digits=15, decimal_places=2, null=True) # Para dinero, puede ser nulo
    direccion = peewee.CharField(null=True)
    lat = peewee.DecimalField(max_digits=10, decimal_places=8, null=True) # Latitud
    lng = peewee.DecimalField(max_digits=11, decimal_places=8, null=True) # Longitud
    fecha_inicio = peewee.DateField(null=True) # Formato YYYY-MM-DD
    fecha_fin_inicial = peewee.DateField(null=True)
    plazo_meses = peewee.IntegerField(null=True)
    porcentaje_avance = peewee.DecimalField(max_digits=5, decimal_places=2, null=True) # Ej: 99.99
    imagen_1 = peewee.CharField(null=True) # URL de imagen
    imagen_2 = peewee.CharField(null=True)
    imagen_3 = peewee.CharField(null=True)
    imagen_4 = peewee.CharField(null=True)
    licitacion_anio = peewee.IntegerField(null=True)
    nro_contratacion = peewee.CharField(null=True)
    beneficiarios = peewee.CharField(null=True) # Texto sobre los beneficiarios
    mano_obra = peewee.IntegerField(null=True)
    compromiso = peewee.CharField(null=True) # Si es un compromiso de gobierno
    destacada = peewee.BooleanField(null=True) # Si es destacada (True/False)
    ba_elige = peewee.BooleanField(null=True) # Si es parte de "BA Elige"
    link_interno = peewee.CharField(null=True) # Link a información interna
    pliego_descarga = peewee.CharField(null=True) # URL de descarga de pliego
    expediente_numero = peewee.CharField(null=True)
    estudio_ambiental_descarga = peewee.CharField(null=True) # URL de descarga de estudio ambiental

    # Relaciones (Foreign Keys) con las tablas de apoyo
    etapa = peewee.ForeignKeyField(Etapa, backref='obras', null=True)
    tipo_obra = peewee.ForeignKeyField(TipoObra, backref='obras', null=True)
    area_responsable = peewee.ForeignKeyField(AreaResponsable, backref='obras', null=True)
    comuna = peewee.ForeignKeyField(Comuna, backref='obras', null=True)
    barrio = peewee.ForeignKeyField(Barrio, backref='obras', null=True)
    contratacion_tipo = peewee.ForeignKeyField(TipoContratacion, backref='obras', null=True)
    licitacion_oferta_empresa = peewee.ForeignKeyField(Empresa, backref='obras_licitadas', null=True)
    financiamiento = peewee.ForeignKeyField(Financiamiento, backref='obras', null=True)


    class Meta:
        db_table = 'obras' # Nombre de la tabla principal en la DB

    # 5. Métodos de Instancia para el Ciclo de Vida de la Obra (Requisito 5)
    # Estos métodos modifican los atributos de la instancia de Obra y deben persistirse con .save()

    def nuevo_proyecto(self):
        """Marca la obra como un nuevo proyecto. Inicializa la etapa a 'Proyecto'."""
        self.etapa, _ = Etapa.get_or_create(nombre="Proyecto") # Asegura que "Proyecto" exista
        # Los atributos tipo_obra, area_responsable, barrio deben ser establecidos al crear la obra,
        # o aquí si tienen un valor inicial por defecto.
        self.save() # Persiste el cambio
        print(f"Obra '{self.nombre}' iniciada como 'Proyecto'.")

    def iniciar_contratacion(self, tipo_contratacion_nombre, nro_contratacion_val):
        """Inicia la etapa de contratación de la obra."""
        self.etapa, _ = Etapa.get_or_create(nombre="Contratación")
        self.contratacion_tipo, _ = TipoContratacion.get_or_create(nombre=tipo_contratacion_nombre)
        self.nro_contratacion = nro_contratacion_val
        self.save()
        print(f"Obra '{self.nombre}': Iniciada Contratación ({self.nro_contratacion}).")

    def adjudicar_obra(self, empresa_nombre, cuit_empresa, nro_expediente_val):
        """Adjudica la obra a una empresa."""
        self.etapa, _ = Etapa.get_or_create(nombre="Adjudicada") # Podrías tener una etapa "Adjudicada"
        self.licitacion_oferta_empresa, _ = Empresa.get_or_create(nombre=empresa_nombre, cuit=cuit_empresa)
        self.expediente_numero = nro_expediente_val
        self.save()
        print(f"Obra '{self.nombre}': Adjudicada a {self.licitacion_oferta_empresa.nombre}.")


    def iniciar_obra(self, destacada_val, fecha_inicio_val, fecha_fin_inicial_val, fuente_financiamiento_nombre, mano_obra_val):
        """Registra el inicio de la obra."""
        self.etapa, _ = Etapa.get_or_create(nombre="En Ejecución")
        self.destacada = destacada_val
        self.fecha_inicio = fecha_inicio_val
        self.fecha_fin_inicial = fecha_fin_inicial_val
        self.financiamiento, _ = Financiamiento.get_or_create(nombre=fuente_financiamiento_nombre)
        self.mano_obra = mano_obra_val
        self.save()
        print(f"Obra '{self.nombre}': Iniciada en {self.fecha_inicio}.")

    def actualizar_porcentaje_avance(self, nuevo_porcentaje):
        """Actualiza el porcentaje de avance de la obra."""
        if not (0 <= nuevo_porcentaje <= 100):
            raise ValueError("El porcentaje de avance debe estar entre 0 y 100.")
        self.porcentaje_avance = nuevo_porcentaje
        self.save()
        print(f"Obra '{self.nombre}': Porcentaje de avance actualizado a {self.porcentaje_avance}%.")

    def incrementar_plazo(self, meses_adicionales):
        """Incrementa el plazo de la obra en meses."""
        if meses_adicionales <= 0:
            raise ValueError("Los meses adicionales deben ser un valor positivo.")
        self.plazo_meses = (self.plazo_meses or 0) + meses_adicionales
        self.save()
        print(f"Obra '{self.nombre}': Plazo incrementado en {meses_adicionales} meses. Nuevo plazo: {self.plazo_meses} meses.")

    def incrementar_mano_obra(self, aumento_mano_obra):
        """Incrementa la cantidad de mano de obra."""
        if aumento_mano_obra <= 0:
            raise ValueError("El aumento de mano de obra debe ser un valor positivo.")
        self.mano_obra = (self.mano_obra or 0) + aumento_mano_obra
        self.save()
        print(f"Obra '{self.nombre}': Mano de obra incrementada en {aumento_mano_obra}. Nueva mano de obra: {self.mano_obra}.")

    def finalizar_obra(self):
        """Marca la obra como finalizada."""
        self.etapa, _ = Etapa.get_or_create(nombre="Finalizada")
        self.porcentaje_avance = 100.00
        self.save()
        print(f"Obra '{self.nombre}' Finalizada al 100%.")

    def rescindir_obra(self):
        """Marca la obra como rescindida."""
        self.etapa, _ = Etapa.get_or_create(nombre="Rescindida")
        self.save()
        print(f"Obra '{self.nombre}' Rescindida.")

# Lista de todos los modelos para facilitar la creación de tablas
MODELOS = [Etapa, TipoObra, AreaResponsable, Comuna, Barrio, TipoContratacion, Empresa, Financiamiento, Obra]
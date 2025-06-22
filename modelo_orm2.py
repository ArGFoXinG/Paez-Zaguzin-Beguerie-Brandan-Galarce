from peewee import *

# Conexión a la base de datos SQLite
db = SqliteDatabase('obras_urbanas.db')

class BaseModel(Model):
    class Meta:
        database = db

class Etapa(BaseModel):
    nombre = CharField(unique=True, null=False)

class TipoObra(BaseModel):
    nombre = CharField(unique=True, null=False)

class AreaResponsable(BaseModel):
    nombre = CharField(unique=True, null=False)

class Comuna(BaseModel):
    numero = IntegerField(unique=True, null=False)

class Barrio(BaseModel):
    nombre = CharField(null=False)
    comuna = ForeignKeyField(Comuna, backref='barrios')

class TipoContratacion(BaseModel):
    nombre = CharField(unique=True, null=False)

class Empresa(BaseModel):
    nombre = CharField(unique=True, null=False)

class Financiamiento(BaseModel):
    nombre = CharField(unique=True, null=False)

class Obra(BaseModel):
    nombre = CharField(null=False)
    descripcion = TextField(null=True)
    direccion = CharField(null=True)
    monto_contrato = FloatField(null=True)
    tipo_obra = ForeignKeyField(TipoObra, backref='obras', null=True)
    area_responsable = ForeignKeyField(AreaResponsable, backref='obras', null=True)
    etapa = ForeignKeyField(Etapa, backref='obras', null=True)
    comuna = ForeignKeyField(Comuna, backref='obras', null=True)
    barrio = ForeignKeyField(Barrio, backref='obras', null=True)
    # Puedes agregar más campos según tu CSV, por ejemplo:
    # plazo_meses = peewee.IntegerField(null=True)
    # imagen = CharField(null=True)
    # licitacion_anio = IntegerField(null=True)
    # contrato_tipo = CharField(null=True)
    # nro_contrato = CharField(null=True)
    # nro_expediente = CharField(null=True)
    # licitacion_presupuesto = DecimalField(auto_round=True, null=True)
    # beneficiarios = CharField(null=True)
    # observaciones = TextField(null=True)
    porcentaje_avance = FloatField(null=True)
    fecha_inicio = DateField(null=True)
    fecha_fin_inicial = DateField(null=True)
    lat = FloatField(null=True)
    lng = FloatField(null=True)

    class Meta:
        db_table = 'obras'  # Nombre de la tabla en la base de datos

    # Métodos de instancia para el ciclo de vida de la obra

    def nuevo_proyecto(self):
        """Marca la obra como un nuevo proyecto (etapa 'Proyecto')."""
        self.etapa, _ = Etapa.get_or_create(nombre="Proyecto")
        self.save()
        print(f"Obra '{self.nombre}' iniciada como 'Proyecto'.")

    def iniciar_contratacion(self, tipo_contratacion_nombre):
        """Inicia la etapa de contratación de la obra."""
        self.etapa, _ = Etapa.get_or_create(nombre="Contratación")
        tipo_contratacion, _ = TipoContratacion.get_or_create(nombre=tipo_contratacion_nombre)
        # Si tu modelo tiene un campo para tipo de contratación, asígnalo aquí
        # self.tipo_contratacion = tipo_contratacion
        self.save()
        print(f"Obra '{self.nombre}': Iniciada Contratación ({tipo_contratacion.nombre}).")

    def finalizar_obra(self):
        """Marca la obra como finalizada."""
        self.etapa, _ = Etapa.get_or_create(nombre="Finalizada")
        self.save()
        print(f"Obra '{self.nombre}' finalizada.")

# Lista de modelos para crear las tablas fácilmente
MODELOS = [Etapa, TipoObra, AreaResponsable, Comuna, Barrio, TipoContratacion, Empresa, Financiamiento, Obra]
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound


def replace_id(table, old, new, id_field, ctx, name=None):
    ctx.logger.info('Parche: replace_id')
    if name:
        ctx.logger.info('+ Nombre: %s', name)
    ctx.logger.info('+ Reemplazar %s con %s (campo: %s)', old, new, id_field)

    try:
        entity = ctx.query(table).filter_by(**{id_field: old}).one()
        setattr(entity, id_field, new)
        ctx.logger.info('+ Parche aplicado.')
    except NoResultFound:
        ctx.logger.warn('+ No se encontró la entidad con %s = %s.', id_field,
                        old)
    except MultipleResultsFound:
        ctx.logger.warn('+ Varias entidades con %s = %s.', id_field, old)

    ctx.logger.info('')


def delete(table, field, value, ctx, name=None):
    ctx.logger.info('Parche: delete')
    if name:
        ctx.logger.info('+ Nombre: %s', name)
    ctx.logger.info('+ Borrar entidades con %s = %s.', field, value)

    count = ctx.query(table).filter_by(**{field: value}).delete()
    if count:
        ctx.logger.info('+ Parche aplicado.')
    else:
        ctx.logger.warn('+ No se borraron entidades.')

    ctx.logger.info('')

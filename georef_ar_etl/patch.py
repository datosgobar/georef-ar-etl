from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound


def update_row_field(table, field, value, ctx, **conditions):
    ctx.logger.info('Parche: update_field')
    ctx.logger.info('+ Modificar la entidad que cumpla con:')
    for k, v in conditions.items():
        ctx.logger.info('  > %s = %s', k, v)
    ctx.logger.info('+ Establecer su %s a: %s.', field, value)

    try:
        row = ctx.query(table).filter_by(**conditions).one()
        setattr(row, field, value)
        ctx.logger.info('+ Parche aplicado.')
    except NoResultFound:
        ctx.logger.warn(
            '+ No se encontró una entidad que cumpla las condiciones.')
    except MultipleResultsFound:
        ctx.logger.warn('+ Varias entidades cumplen la condición.')

    ctx.logger.info('')


def delete(table, ctx, **conditions):
    ctx.logger.info('Parche: delete')
    ctx.logger.info('+ Eliminar entidades que cumplan con:')
    for k, v in conditions.items():
        ctx.logger.info('  > %s = %s', k, v)

    count = ctx.query(table).filter_by(**conditions).delete()
    if count:
        ctx.logger.info('+ Parche aplicado (entidades eliminadas: %s).', count)
    else:
        ctx.logger.warn('+ No se borraron entidades.')

    ctx.logger.info('')

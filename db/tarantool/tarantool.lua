box.cfg{
    listen = 3301,
}

box.schema.space.create('test')
box.space.test:format({
    { name = 'id', type = 'string' },
    { name = 'value', type = 'string' },
})

box.space.test:create_index('primary', {
    type = 'hash',
    unique = true,
    parts = { 
        {field = 1, type = 'string'},
     }
})

box.space.test:truncate()

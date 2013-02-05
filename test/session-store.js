
var should = require('should');

var client = require('triton').open({
	host: '127.0.0.1',
	port: 4848,
	cassandra: {
		session : {
			cluster : 'test',
			keyspace : 'proteus_session'
		}
	}
});

var session = require('../')(
	client.keyspaces.session,
	{
		secret: 'ABCDEFG'
	}
);

describe('session', function() {

	it('create ticket', function(done) {

		session.createTicket('100', function(err, ticket) {
			should.exist(ticket);
			done(err);
		});

	}),
	
	it('get ticket user', function(done) {

		session.createTicket('100', function(err, ticket) {
			session.getId(ticket, function(err, id) {
				should.exist(id);
				id.should.equal('100');
				done(err);
			});
		});

	});

	it('store session', function(done) {

		session.createTicket('100', function(err, ticket) {
			session.getId(ticket, function(err, id) {
				session.setSession(id, { name: 'test', value: 100 }, function(err) {
					session.getSession(id, ['name','value'], function(err, data) {
						should.exist(data);
						data.should.have.property('name');
						data.should.have.property('value');
						data.name.should.equal('test');
						data.value.should.equal(100);
						done(err);
					});
				});
			});
		});

	});

	it('exchange token', function(done) {

		session.createTicket('100', function(err, ticket) {
			should.exist(ticket);
			session.createToken(ticket, function(err, token) {
				should.exist(token);
				session.exchangeToken(token, function(err, exchangedTicket) {
					should.exist(exchangedTicket);
					exchangedTicket.should.equal(ticket);
					done();
				});
			});
		});

	});

});

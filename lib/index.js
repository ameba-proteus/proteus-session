
var triton = require('triton');
var crypto = require('crypto');

function ProteusSession(keyspace, config) {
	this.keyspace = keyspace;
	this.config = config;
	this.readyHandlers = [];
	this.prepare();
}
ProteusSession.prototype = {

	/**
	 * prepare for the column family
	 */
	prepare: function() {
		var self = this;
		var config = self.config;
		var keyspace = self.keyspace;

		var storeFamily = keyspace.family(config.storeFamily || 'session_store');
		var ticketFamily = keyspace.family(config.ticketFamily || 'session_ticket');

		function invokeReady(err, result) {
			// invoke ready handlers
			for (var i = 0; i < self.readyHandlers.length; i++) {
				self.readyHandlers[i].call(self, err, result);
			}
		}

		createFamily(storeFamily, function(err) {
			if (err) {
				self.family = err;
				invokeReady(err);
				return;
			}
			createFamily(ticketFamily, function(err) {
				if (err) {
					self.family = err;
					invokeReady(err);
					return;
				}
				self.family = {
					store: storeFamily,
					ticket: ticketFamily
				};
				invokeReady(null, self.family);
			});
		});

		function createFamily(family, callback) {
			family.get({key:'_'}, function(err) {
				if (err && err.code === triton.errors.cassandra.no_column_family) {
					// create column family if not exist
					family.create({
						key_validation_class: 'UTF8Type',
						comparator: 'UTF8Type',
						default_validation_class: 'UTF8Type'
					}, function(err) {
						// ignore error
						if (err) {
							console.error(err);
						}
						callback();
					});
				} else {
					// set current family
					self.family = family;
					callback();
				}
			});
		}
	},

	/**
	 * Callback when session store is ready
	 */
	ready: function(callback) {
		if (this.family) {
			// error
			if ('name' in this.family) {
				return callback(this.family);
			} else {
				return callback(null, this.family);
			}
		}
		this.readyHandlers.push(callback);
	},

	/**
	 * Create ticket related to user id.
	 * It will returns ticket if user has
	 * another ticket which has been issued
	 * in other environments.
	 *
	 * @param {String} userId
	 * @param {Function} callback
	 */
	createTicket: function(id, callback) {

		var self = this;
		var config = self.config;
		// default 30 days to expire
		var ttl = config.expire || 60*60*24*30;

		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			(function create() {
				var ticket = crypto.randomBytes(24).toString('base64');
				family.ticket.get({ key: ticket, columns: ['id'] }, function(err, row) {
					if (err) {
						return callback(err);
					}
					if (row === null) {
						// create
						var sets = {};
						sets[ticket] = { id: id };
						// expires in
						family.ticket.set(sets, { ttl: ttl }, function(err) {
							if (err) {
								callback(err);
							} else {
								callback(null, ticket);
							}
						});
					} else {
						// re-create ticket if row exists
						create();
					}
				});
			})();
		});

	},

	/**
	 * Get ID from the ticket
	 * @param {String} ticket
	 * @param {Function} callback function(err, id)
	 */
	getId: function(ticket, callback) {
		var self = this;
		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			family.ticket.get({ key: ticket, columns: ['id']}, function(err, row) {
				if (err) {
					return callback(err);
				}
				callback(null, row ? row.id : null);
			});
		});
	},

	/**
	 * Save session
	 * @param {String} id id of the data
	 * @param {String} data column mapping
	 * @param {Function} callback function(err)
	 */
	setSession: function(id, data, callback) {
		var self = this;
		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			var sets = {};
			sets[id] = data;
			family.store.set(sets, function(err) {
				callback(err);
			});
		});
	},

	/**
	 * Get session from the ticket
	 * @param {String} ticket Ticket string
	 * @param {Object} columns Column map for save.
	 * @param {Function} callback function(err, row) row will be null if ticket is invalid.
	 */
	getTicketSession: function(ticket, columns, callback) {
		var self = this;
		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			family.ticket.get({ key: ticket, columns: ['id'] }, function(err, row) {
				if (err) {
					return callback(err);
				}
				// return null if row is not exist
				if (!row) {
					return callback(null, null);
				}
				var id = row.id;
				family.store.get({ key: id, columns: columns }, function(err, row) {
					if (err) {
						return callback(err);
					}
					row = row || {};
					row.id = id;
					callback(null, row);
				});
			});
		});
	},

	/**
	 * Get session
	 * @param {String} id Ticket ID
	 * @param {Object} columns Array of column names to get
	 * @param {Function} callback function(err, row)
	 */
	getSession: function(id, columns, callback) {
		var self = this;
		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			family.store.get({ key: id, columns: columns }, function(err, row) {
				if (err) {
					return callback(err);
				}
				callback(null,row);
			});
		});
	},

	/**
	 * Create a token for exchanging from
	 * the another device.
	 */
	createToken: function(ticket, callback) {
		var self = this;
		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			family.ticket.get({ key: ticket, columns: ['id'] }, function(err, row) {
				if (err) {
					return callback(err);
				}
				if (!row) {
					return callback(new Error('Wrong ticket'));
				}
				var nums = "abcdefghijklmnopqrstuvwxyz";
				var token = '';
				for (var i = 0; i < 8; i++) {
					token += nums[Math.floor(Math.random()*nums.length)];
				}
				var sets = {};
				sets[token] = { ticket: ticket };
				family.ticket.set(sets, {
					ttl: 300 // 5 minutes to expire
				}, function(err) {
					callback(err, token);
				});
			});
		});
	},

	/**
	 * Exchange token to the ticket.
	 */
	exchangeToken: function(token, callback) {
		var self = this;
		self.ready(function(err, family) {
			if (err) {
				return callback(err);
			}
			family.ticket.get({ key: token, columns: ['ticket'] }, function(err, row) {
				if (err) {
					return callback(err);
				}
				if (!row) {
					return callback(new Error('Wrong token'));
				}
				callback(null, row.ticket);
			});
		});
	},

	/**
	 * Connect middleware for the ticket
	 */
	middleware: function(option) {

		option = option || {};
		var self = this;
		var header = option.header || 'X-Proteus-Session';
		var columns = option.columns || [];

		return function(req, res, next) {
			var ticket = req.get(header);
			if (ticket) {
				self.getTicketSession(ticket, columns, function(err, row) {
					if (err) {
						next(err);
					} else {
						if (row) {
							row.ticket = ticket;
							req.session = row;
						}
						next();
					}
				});
			} else {
				next();
			}
		};

	}

};

module.exports = function(keyspace, config) {
	return new ProteusSession(keyspace, config);
};

/*
module.exports = function() {
	vars = vars || {};
	var cassandra = vars.cassandra;
	if (!cassandra) {
		throw 'cassandra instance not specified';
	}
	var secret = vars.secret;
	if (!secret) {
		throw 'secret key not specified';
	}
	var header = (vars.header || 'X-Proteus-Session').toLowerCase();
	var expireTime = vars.expireTime || 60*60*24*1000;
	var sessionCF = vars.sessionCF || 'sessions';
	var anonymousCF = vars.anonymousCF || 'anonymous_sessions';
	var getUserID = vars.getUserID || function(req) {
		return req.userID;
	};
	var getUserSecret = vars.getUserSecret || createUserSecret;
	store = new Store();
	storeAnonymous = new Store();
	var ready = false;
	store.init(cassandra, sessionCF, function(err) {
		if (err) {
			logger.error('session manager init error', { message: err.message, stack: err.stack });
		} else {
			storeAnonymous.init(cassandra, anonymousCF, function(err) {
				if (err) {
					logger.error('session manager init error', { message: err.message, stack: err.stack });
				} else {
					logger.info('session manager initialized');
					ready = true;
				}
			});
		}
	});
	return function (req, res, next) {
		if (!ready) {
			logger.debug('session store not ready');
			return next();
		}
		// find session ID
		var sid = req.headers[header];
		logger.debug('session ID' + sid);
		var expire = null;
		var userID = null;
		var generateSession = function(userID, userSecret) {
			var isAnonymous = userID === null;
			var expire = new Date().getTime() + expireTime;
			var sid = encryption.createSessionID(
				secret, userID, userSecret, expire);
			var session = new Session(isAnonymous ? storeAnonymous : store, sid, userID, expire);
			return session;
		};
		var setHeaderAndNext = function(sessionID, req) {
			res.on('header', function() {
				res.setHeader(header, sessionID);
			});
			next();
		};
		var info = null;
		var regenerate = false;
		if (sid) {
			info = encryption.extractData(secret, sid);
			logger.debug('session info', info);
			userID = info.userID;
		} else {
			userID = getUserID(req);
			regenerate = true;
		}
		if (userID) {
			getUserSecret(userID, function(userSecret) {
				if (info) {
					expire = info.expire;
					var now = new Date().getTime();
					// validation
					if (
						now > expire ||
						!encryption.validateHash(info.hash, userID, userSecret, expire)
					) {
						regenerate = true;
					}
				}
				if (regenerate) {
					var session = generateSession(userID, userSecret);
					req.session = session;
					sid = session.id;
				}
				setHeaderAndNext(sid, req);
			});
		} else {
			var session = generateSession(null, null);
			req.session = session;
			sid = session.id;
			setHeaderAndNext(sid, req);
		}
		return null;
	};
};

var createUserSecret = function(userID, callback) {
	var salt = 'aqwsedrftg';
	var hash = encryption.createHash(salt, userID);
	callback(hash);
};
*/

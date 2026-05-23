
const ParameterMutator = require('./parameterMutator');
const HeaderMutator = require('./headerMutator');

class MutationEngine {

    constructor() {
        this.parameterMutator = new ParameterMutator();
        this.headerMutator = new HeaderMutator();
    }

    mutate(request) {
        const allMutations = [];

        // Run parameter mutations if request has params
        if (request.params && Object.keys(request.params).length > 0) {
            const paramMutations = this.parameterMutator.mutate(request.params);
            allMutations.push(...paramMutations);
        }

        // Run header mutations if request has headers
        if (request.headers && Object.keys(request.headers).length > 0) {
            const headerMutations = this.headerMutator.mutate(request.headers);
            allMutations.push(...headerMutations);
        }

        return allMutations;
    }
}

module.exports = MutationEngine;
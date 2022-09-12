import { Storage } from '../storage/storage';
import {
	ModelInstanceMetadata,
	OpType,
	PersistentModelConstructor,
	SchemaModel,
} from '../types';
import { MutationEventOutbox } from './outbox';
import { getIdentifierValue } from './utils';
import {
	extractPrimaryKeyFieldNames,
	extractPrimaryKeysAndValues,
} from '../util';
import { ModelPredicateCreator } from '../predicates';

// https://github.com/aws-amplify/amplify-js/blob/datastore-docs/packages/datastore/docs/sync-engine.md#merger
class ModelMerger {
	constructor(
		private readonly outbox: MutationEventOutbox,
		private readonly ownSymbol: Symbol
	) {}

	public async merge<T extends ModelInstanceMetadata>(
		storage: Storage,
		model: T,
		modelDefinition: SchemaModel,
		modelConstructor: PersistentModelConstructor<T>
	): Promise<OpType | undefined> {
		let result: OpType;
		const mutationsForModel = await this.outbox.getForModel(
			storage,
			model,
			modelDefinition
		);

		const isDelete = model._deleted;
		const modelPkFields = extractPrimaryKeyFieldNames(modelDefinition);
		const identifierObject = extractPrimaryKeysAndValues(model, modelPkFields);
		const predicate = ModelPredicateCreator.createForPk<T>(
			modelDefinition,
			identifierObject
		);

		if (mutationsForModel.length > 0) {
			await this.outbox.updateModelVersion(storage, model, modelDefinition);
			return;
		}

		const [fromDB] = await storage.query(modelConstructor, predicate);

		if (
			!fromDB ||
			fromDB._version === undefined ||
			fromDB._version < model._version
		) {
			if (isDelete) {
				result = OpType.DELETE;
				await storage.delete(model, undefined, this.ownSymbol);
			} else {
				[[, result]] = await storage.save(model, undefined, this.ownSymbol);
			}

			await this.outbox.deleteModelVersion(storage, model, modelDefinition);
			return result;
		}

		return undefined;
	}

	public async mergePage(
		storage: Storage,
		modelConstructor: PersistentModelConstructor<any>,
		items: ModelInstanceMetadata[],
		modelDefinition: SchemaModel
	): Promise<[ModelInstanceMetadata, OpType][]> {
		const itemsMap: Map<string, ModelInstanceMetadata> = new Map();

		for (const item of items) {
			// merge items by model id. Latest record for a given id remains.
			const modelId = getIdentifierValue(modelDefinition, item);

			itemsMap.set(modelId, item);
		}

		const page = [...itemsMap.values()];

		return await storage.batchSave(modelConstructor, page, this.ownSymbol);
	}
}

export { ModelMerger };

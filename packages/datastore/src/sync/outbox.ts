import { MutationEvent, PendingMutationVersion } from './index';
import { ModelPredicateCreator } from '../predicates';
import {
	ExclusiveStorage as Storage,
	StorageFacade,
	Storage as StorageClass,
} from '../storage/storage';
import { ModelInstanceCreator } from '../datastore/datastore';
import {
	InternalSchema,
	PersistentModel,
	PersistentModelConstructor,
	QueryOne,
	SchemaModel,
} from '../types';
import { USER, SYNC, valuesEqual } from '../util';
import { getIdentifierValue, TransformerMutationType } from './utils';

// TODO: Persist deleted ids
// https://github.com/aws-amplify/amplify-js/blob/datastore-docs/packages/datastore/docs/sync-engine.md#outbox
class MutationEventOutbox {
	private inProgressMutationEventId?: string;

	constructor(
		private readonly schema: InternalSchema,
		private readonly MutationEvent: PersistentModelConstructor<MutationEvent>,
		private readonly PendingMutationVersion: PersistentModelConstructor<PendingMutationVersion>,
		private readonly modelInstanceCreator: ModelInstanceCreator,
		private readonly ownSymbol: Symbol
	) {}

	public async enqueue(
		storage: Storage,
		mutationEvent: MutationEvent
	): Promise<void> {
		storage.runExclusive(async s => {
			const mutationEventModelDefinition =
				this.schema.namespaces[SYNC].models['MutationEvent'];

			// `id` is the id of the record in the mutationEvent
			// `modelId` is the id of the actual record that was mutated
			const predicate = ModelPredicateCreator.createFromExisting<MutationEvent>(
				mutationEventModelDefinition,
				c =>
					c
						.modelId('eq', mutationEvent.modelId)
						.id('ne', this.inProgressMutationEventId)
			);

			// Check if there are any other records with same id
			const [first] = await s.query(this.MutationEvent, predicate);

			// No other record with same modelId, so enqueue
			if (first === undefined) {
				await s.save(mutationEvent, undefined, this.ownSymbol);
				return;
			}

			// There was an enqueued mutation for the modelId, so continue
			const { operation: incomingMutationType } = mutationEvent;

			if (first.operation === TransformerMutationType.CREATE) {
				if (incomingMutationType === TransformerMutationType.DELETE) {
					await s.delete(this.MutationEvent, predicate);
				} else {
					// first gets updated with the incoming mutation's data, condition intentionally skipped

					// we need to merge the fields for a create and update mutation to prevent
					// data loss, since update mutations only include changed fields
					const merged = this.mergeUserFields(first, mutationEvent);
					await s.save(
						this.MutationEvent.copyOf(first, draft => {
							draft.data = merged.data;
						}),
						undefined,
						this.ownSymbol
					);
				}
			} else {
				const { condition: incomingConditionJSON } = mutationEvent;
				const incomingCondition = JSON.parse(incomingConditionJSON);
				let merged: MutationEvent;

				// If no condition
				if (Object.keys(incomingCondition).length === 0) {
					merged = this.mergeUserFields(first, mutationEvent);

					// delete all for model
					await s.delete(this.MutationEvent, predicate);
				}

				merged = merged || mutationEvent;

				// Enqueue new one
				await s.save(merged, undefined, this.ownSymbol);
			}
		});
	}

	public async dequeue(
		storage: StorageClass
		// record?: PersistentModel,
		// recordOp?: TransformerMutationType
	): Promise<MutationEvent | undefined> {
		const head = await this.peek(storage);

		// if (record) {
		// 	await this.syncOutboxVersionsOnDequeue(storage, record, head, recordOp);
		// }

		if (head) await storage.delete(head);
		this.inProgressMutationEventId = undefined;

		return head;
	}

	/**
	 * Doing a peek() implies that the mutation goes "inProgress"
	 *
	 * @param storage
	 */
	public async peek(
		storage: StorageFacade
	): Promise<MutationEvent | undefined> {
		const head = await storage.queryOne(this.MutationEvent, QueryOne.FIRST);

		this.inProgressMutationEventId = head ? head.id : undefined;

		return head;
	}

	public async getForModel<T extends PersistentModel>(
		storage: StorageFacade,
		model: T,
		userModelDefinition: SchemaModel
	): Promise<MutationEvent[]> {
		const mutationEventModelDefinition =
			this.schema.namespaces[SYNC].models.MutationEvent;

		const modelId = getIdentifierValue(userModelDefinition, model);

		const mutationEvents = await storage.query(
			this.MutationEvent,
			ModelPredicateCreator.createFromExisting(
				mutationEventModelDefinition,
				c => c.modelId('eq', modelId)
			)
		);

		return mutationEvents;
	}

	public async getModelIds(storage: StorageFacade): Promise<Set<string>> {
		const mutationEvents = await storage.query(this.MutationEvent);

		const result = new Set<string>();

		mutationEvents.forEach(({ modelId }) => result.add(modelId));

		return result;
	}

	public async updateModelVersion<T extends PersistentModel>(
		storage: StorageFacade,
		model: T,
		userModelDefinition: SchemaModel
	): Promise<void> {
		const pendingMutationVersion = await this.getModelVersion(
			storage,
			model,
			userModelDefinition
		);

		if (pendingMutationVersion === undefined) {
			const modelId = getIdentifierValue(userModelDefinition, model);

			await storage.save(
				new this.PendingMutationVersion({
					id: modelId,
					version: model._version,
				})
			);
			return;
		}

		await storage.save(
			this.PendingMutationVersion.copyOf(pendingMutationVersion, updated => {
				updated.version = model._version;
			})
		);
	}

	public async deleteModelVersion<T extends PersistentModel>(
		storage: StorageFacade,
		model: T,
		userModelDefinition: SchemaModel
	): Promise<void> {
		const pendingMutationVersion = await this.getModelVersion(
			storage,
			model,
			userModelDefinition
		);

		if (pendingMutationVersion !== undefined) {
			await storage.delete(pendingMutationVersion);
		}
	}

	public async getModelVersion<T extends PersistentModel>(
		storage: StorageFacade,
		model: T,
		userModelDefinition: SchemaModel
	): Promise<PendingMutationVersion | undefined> {
		const modelId = getIdentifierValue(userModelDefinition, model);

		const pendingMutationVersionModelDefinition =
			this.schema.namespaces[SYNC].models['PendingMutationVersion'];

		const predicate =
			ModelPredicateCreator.createFromExisting<PendingMutationVersion>(
				pendingMutationVersionModelDefinition,
				c => c.id('eq', modelId)
			);

		const [pendingMutationVersion] = await storage.query(
			this.PendingMutationVersion,
			predicate
		);

		return pendingMutationVersion;
	}

	private mergeUserFields(
		previous: MutationEvent,
		current: MutationEvent
	): MutationEvent {
		const { _version, _lastChangedAt, _deleted, ...previousData } =
			previous.data;

		const {
			_version: __version,
			_lastChangedAt: __lastChangedAt,
			_deleted: __deleted,
			...currentData
		} = current.data;

		const data = {
			_version,
			_lastChangedAt,
			_deleted,
			...previousData,
			...currentData,
		};

		return this.modelInstanceCreator(this.MutationEvent, {
			...current,
			data,
		});
	}
}

export { MutationEventOutbox };

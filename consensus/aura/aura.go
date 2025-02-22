// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package aura

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura/aurainterfaces"
	"github.com/ledgerwatch/erigon/consensus/aura/contracts"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/secp256k1"
	"go.uber.org/atomic"
)

/*
Not implemented features from OS:
 - two_thirds_majority_transition - because no chains in OE where this is != MaxUint64 - means 1/2 majority used everywhere
 - emptyStepsTransition - same

*/

type StepDurationInfo struct {
	TransitionStep      uint64
	TransitionTimestamp uint64
	StepDuration        uint64
}

// Holds 2 proofs inside: ValidatorSetProof and FinalityProof
type EpochTransitionProof struct {
	SignalNumber  uint64
	SetProof      []byte
	FinalityProof []byte
}

// SetProof - validator set proof
type ValidatorSetProof struct {
	Header   *types.Header
	Receipts types.Receipts
}
type EpochTransition struct {
	/// Block hash at which the transition occurred.
	BlockHash common.Hash
	/// Block number at which the transition occurred.
	BlockNumber uint64
	/// "transition/epoch" proof from the engine combined with a finality proof.
	ProofRlp []byte
}

type Step struct {
	calibrate bool // whether calibration is enabled.
	inner     *atomic.Uint64
	// Planned durations of steps.
	durations []StepDurationInfo
}

func (s *Step) doCalibrate() {
	if s.calibrate {
		if !s.optCalibrate() {
			ctr := s.inner.Load()
			panic(fmt.Errorf("step counter under- or overflow: %d", ctr))
		}
	}
}

// optCalibrate Calibrates the AuRa step number according to the current time.
func (s *Step) optCalibrate() bool {
	now := time.Now().Second()
	var info StepDurationInfo
	i := 0
	for _, d := range s.durations {
		if d.TransitionTimestamp >= uint64(now) {
			break
		}
		info = d
		i++
	}
	if i == 0 {
		panic("durations cannot be empty")
	}

	if uint64(now) < info.TransitionTimestamp {
		return false
	}

	newStep := (uint64(now)-info.TransitionTimestamp)/info.StepDuration + info.TransitionStep
	s.inner.Store(newStep)
	return true
}

type PermissionedStep struct {
	inner      *Step
	canPropose *atomic.Bool
}

type ReceivedStepHashes map[uint64]map[common.Address]common.Hash //BTreeMap<(u64, Address), H256>

//nolint
func (r ReceivedStepHashes) get(step uint64, author common.Address) (common.Hash, bool) {
	res, ok := r[step]
	if !ok {
		return common.Hash{}, false
	}
	result, ok := res[author]
	return result, ok
}

//nolint
func (r ReceivedStepHashes) insert(step uint64, author common.Address, blockHash common.Hash) {
	res, ok := r[step]
	if !ok {
		res = map[common.Address]common.Hash{}
		r[step] = res
	}
	res[author] = blockHash
}

//nolint
func (r ReceivedStepHashes) dropAncient(step uint64) {
	for i := range r {
		if i < step {
			delete(r, i)
		}
	}
}

//nolint
type EpochManager struct {
	epochTransitionHash   common.Hash // H256,
	epochTransitionNumber uint64      // BlockNumber
	finalityChecker       RollingFinality
	force                 bool
}

func NewEpochManager() *EpochManager {
	return &EpochManager{
		finalityChecker: NewRollingFinality([]common.Address{}),
		force:           true,
	}
}

// zoomValidators - Zooms to the epoch after the header with the given hash. Returns true if succeeded, false otherwise.
// It's analog of zoom_to_after function in OE, but doesn't require external locking
//nolint
func (e *EpochManager) zoomToAfter(chain consensus.ChainHeaderReader, validators ValidatorSet, hash common.Hash) (RollingFinality, uint64, bool) {
	var lastWasParent bool
	if e.finalityChecker.lastPushed != nil {
		lastWasParent = *e.finalityChecker.lastPushed == hash
	}

	// early exit for current target == chain head, but only if the epochs are
	// the same.
	if lastWasParent && !e.force {
		return e.finalityChecker, e.epochTransitionNumber, true
	}
	e.force = false

	// epoch_transition_for can be an expensive call, but in the absence of
	// forks it will only need to be called for the block directly after
	// epoch transition, in which case it will be O(1) and require a single
	// DB lookup.
	lastTransition, ok := epochTransitionFor(chain, hash)
	if !ok {
		return e.finalityChecker, e.epochTransitionNumber, false
	}
	// extract other epoch set if it's not the same as the last.
	if lastTransition.BlockHash != e.epochTransitionHash {
		proof, err := destructure_proofs(lastTransition.ProofRlp)
		if err != nil {
			panic(err)
		}
		first := proof.SignalNumber == 0
		// use signal number so multi-set first calculation is correct.
		list, _, err := validators.epochSet(first, proof.SignalNumber, proof.SetProof)
		if err != nil {
			panic(fmt.Errorf("proof produced by this engine; therefore it is valid; qed. %w", err))
		}
		epochSet := list.validators
		e.finalityChecker = NewRollingFinality(epochSet)
	}
	e.epochTransitionHash = lastTransition.BlockHash
	e.epochTransitionNumber = lastTransition.BlockNumber
	return e.finalityChecker, e.epochTransitionNumber, true
}
func destructure_proofs(b []byte) (EpochTransitionProof, error) {
	res := &EpochTransitionProof{}
	err := rlp.DecodeBytes(b, res)
	if err != nil {
		return EpochTransitionProof{}, err
	}
	return *res, nil
}

/// Get the transition to the epoch the given parent hash is part of
/// or transitions to.
/// This will give the epoch that any children of this parent belong to.
///
/// The block corresponding the the parent hash must be stored already.
//nolint
func epochTransitionFor(chain consensus.ChainHeaderReader, parentHash common.Hash) (transition EpochTransition, ok bool) {
	// slow path: loop back block by block
	for {
		h := chain.GetHeaderByHash(parentHash)
		if h == nil {
			return transition, false
		}

		// look for transition in database.
		transition, ok = epochTransition(h.Number.Uint64(), h.Hash())
		if ok {
			return transition, true
		}

		// canonical hash -> fast breakout:
		// get the last epoch transition up to this block.
		//
		// if `block_hash` is canonical it will only return transitions up to
		// the parent.
		canonical := chain.GetHeaderByNumber(h.Number.Uint64())
		if canonical == nil {
			return transition, false
		}
		//nolint
		if canonical.Hash() == parentHash {

			return EpochTransition{
				BlockNumber: 0,
				BlockHash:   common.HexToHash("0x5b28c1bfd3a15230c9a46b399cd0f9a6920d432e85381cc6a140b06e8410112f"),
				ProofRlp:    params.SokolGenesisEpochProof,
			}, true
			/* TODO:
			   return self
			       .epoch_transitions()
			       .map(|(_, t)| t)
			       .take_while(|t| t.block_number <= details.number)
			       .last();
			*/
		}

		parentHash = h.Hash()
	}
}

// epochTransition get a specific epoch transition by block number and provided block hash.
//nolint
func epochTransition(blockNum uint64, blockHash common.Hash) (transition EpochTransition, ok bool) {
	if blockNum == 0 {
		return EpochTransition{BlockNumber: 0, BlockHash: params.SokolGenesisHash, ProofRlp: params.SokolGenesisEpochProof}, true
	}
	return EpochTransition{}, false
	/*
		pub fn epoch_transition(&self, block_num: u64, block_hash: H256) -> Option<EpochTransition> {
		   trace!(target: "blockchain", "Loading epoch transition at block {}, {}",
		    block_num, block_hash);

		   self.db
		       .key_value()
		       .read(db::COL_EXTRA, &block_num)
		       .and_then(|transitions: EpochTransitions| {
		           transitions
		               .candidates
		               .into_iter()
		               .find(|c| c.block_hash == block_hash)
		       })
		}
	*/
}

//nolint
type unAssembledHeader struct {
	h common.Hash // H256
	n uint64      // BlockNumber
	a []common.Address
}

// RollingFinality checker for authority round consensus.
// Stores a chain of unfinalized hashes that can be pushed onto.
//nolint
type RollingFinality struct {
	headers    []unAssembledHeader //nolint
	signers    *SimpleList
	signCount  map[common.Address]uint
	lastPushed *common.Hash // Option<H256>,
}

/// Create a blank finality checker under the given validator set.
func NewRollingFinality(signers []common.Address) RollingFinality {
	return RollingFinality{
		signers: NewSimpleList(signers),
		//headers: VecDeque::new(),
		//sign_count: HashMap::new(),
		//last_pushed: None
	}
}

// AuRa
//nolint
type AuRa struct {
	db     ethdb.RwKV // Database to store and retrieve snapshot checkpoints
	exitCh chan struct{}
	lock   sync.RWMutex // Protects the signer fields

	step PermissionedStep
	// History of step hashes recently received from peers.
	receivedStepHashes ReceivedStepHashes

	OurSigningAddress common.Address // Same as Etherbase in Mining
	cfg               AuthorityRoundParams
	EmptyStepsSet     *EmptyStepSet
	EpochManager      *EpochManager // Mutex<EpochManager>,

	//Validators                     ValidatorSet
	//ValidateScoreTransition        uint64
	//ValidateStepTransition         uint64
	//immediateTransitions           bool
	//blockReward                    map[uint64]*uint256.Int
	//blockRewardContractTransitions BlockRewardContractList
	//maximumUncleCountTransition    uint64
	//maximumUncleCount              uint
	//maximumEmptySteps              uint
	////machine: EthereumMachine,
	//// If set, enables random number contract integration. It maps the transition block to the contract address.
	//randomnessContractAddress map[uint64]common.Address
	//// The addresses of contracts that determine the block gas limit.
	//blockGasLimitContractTransitions map[uint64]common.Address
	//// Memoized gas limit overrides, by block hash.
	//gasLimitOverrideCache *GasLimitOverride //Mutex<LruCache<H256, Option<U256>>>,
	//// The block number at which the consensus engine switches from AuRa to AuRa with POSDAO
	//// modifications. For details about POSDAO, see the whitepaper:
	//// https://www.xdaichain.com/for-validators/posdao-whitepaper
	//posdaoTransition *uint64 // Option<BlockNumber>,
}

type GasLimitOverride struct {
	cache *lru.Cache
}

func NewGasLimitOverride() *GasLimitOverride {
	// The number of recent block hashes for which the gas limit override is memoized.
	const GasLimitOverrideCacheCapacity = 10

	cache, err := lru.New(GasLimitOverrideCacheCapacity)
	if err != nil {
		panic("error creating prefetching cache for blocks")
	}
	return &GasLimitOverride{cache: cache}
}

func (pb *GasLimitOverride) Pop(hash common.Hash) *uint256.Int {
	if val, ok := pb.cache.Get(hash); ok && val != nil {
		pb.cache.Remove(hash)
		if v, ok := val.(*uint256.Int); ok {
			return v
		}
	}
	return nil
}

func (pb *GasLimitOverride) Add(hash common.Hash, b *uint256.Int) {
	if b == nil {
		return
	}
	pb.cache.ContainsOrAdd(hash, b)
}

func NewAuRa(config *params.AuRaConfig, db ethdb.RwKV, ourSigningAddress common.Address, engineParamsJson []byte) (*AuRa, error) {
	spec := JsonSpec{}
	err := json.Unmarshal(engineParamsJson, &spec)
	if err != nil {
		return nil, err
	}
	auraParams, err := FromJson(spec)
	if err != nil {
		return nil, err
	}

	if _, ok := auraParams.StepDurations[0]; !ok {
		return nil, fmt.Errorf("authority Round step 0 duration is undefined")
	}
	for _, v := range auraParams.StepDurations {
		if v == 0 {
			return nil, fmt.Errorf("authority Round step 0 duration is undefined")
		}
	}
	if _, ok := auraParams.StepDurations[0]; !ok {
		return nil, fmt.Errorf("authority Round step duration cannot be 0")
	}
	//shouldTimeout := auraParams.StartStep == nil
	initialStep := uint64(0)
	if auraParams.StartStep != nil {
		initialStep = *auraParams.StartStep
	}
	var durations []StepDurationInfo
	durInfo := StepDurationInfo{
		TransitionStep:      0,
		TransitionTimestamp: 0,
		StepDuration:        auraParams.StepDurations[0],
	}
	durations = append(durations, durInfo)
	var i = 0
	for time, dur := range auraParams.StepDurations {
		if i == 0 { // skip first
			i++
			continue
		}

		step, t, ok := nextStepTimeDuration(durInfo, time)
		if !ok {
			return nil, fmt.Errorf("timestamp overflow")
		}
		durInfo.TransitionStep = step
		durInfo.TransitionTimestamp = t
		durInfo.StepDuration = dur
		durations = append(durations, durInfo)
	}
	step := &Step{
		inner:     atomic.NewUint64(initialStep),
		calibrate: auraParams.StartStep == nil,
		durations: durations,
	}
	step.doCalibrate()

	/*
		    let engine = Arc::new(AuthorityRound {
		        epoch_manager: Mutex::new(EpochManager::blank()),
		        received_step_hashes: RwLock::new(Default::default()),
		        gas_limit_override_cache: Mutex::new(LruCache::new(GAS_LIMIT_OVERRIDE_CACHE_CAPACITY)),
		    })
			// Do not initialize timeouts for tests.
		    if should_timeout {
		        let handler = TransitionHandler {
		            step: engine.step.clone(),
		            client: engine.client.clone(),
		        };
		        engine
		            .transition_service
		            .register_handler(Arc::new(handler))?;
		    }
	*/

	exitCh := make(chan struct{})

	c := &AuRa{
		db:                 db,
		exitCh:             exitCh,
		step:               PermissionedStep{inner: step, canPropose: atomic.NewBool(true)},
		OurSigningAddress:  ourSigningAddress,
		cfg:                auraParams,
		receivedStepHashes: ReceivedStepHashes{},
		EpochManager:       NewEpochManager(),
	}
	_ = config

	return c, nil
}

// A helper accumulator function mapping a step duration and a step duration transition timestamp
// to the corresponding step number and the correct starting second of the step.
func nextStepTimeDuration(info StepDurationInfo, time uint64) (uint64, uint64, bool) {
	stepDiff := time + info.StepDuration
	if stepDiff < 1 {
		return 0, 0, false
	}
	stepDiff -= 1
	if stepDiff < info.TransitionTimestamp {
		return 0, 0, false
	}
	stepDiff -= info.TransitionTimestamp
	if info.StepDuration == 0 {
		return 0, 0, false
	}
	stepDiff /= info.StepDuration
	timeDiff := stepDiff * info.StepDuration
	return info.TransitionStep + stepDiff, info.TransitionTimestamp + timeDiff, true
}

// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
func (c *AuRa) Author(header *types.Header) (common.Address, error) {
	/*
	 let message = keccak(empty_step_rlp(self.step, &self.parent_hash));
	        let public = publickey::recover(&self.signature.into(), &message)?;
	        Ok(publickey::public_to_address(&public))
	*/
	return common.Address{}, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *AuRa) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, _ bool) error {
	return nil
}

//nolint
func (c *AuRa) hasReceivedStepHashes(step uint64, author common.Address, newHash common.Hash) bool {
	/*
		self
			       .received_step_hashes
			       .read()
			       .get(&received_step_key)
			       .map_or(false, |h| *h != new_hash)
	*/
	return false
}

//nolint
func (c *AuRa) insertReceivedStepHashes(step uint64, author common.Address, newHash common.Hash) {
	/*
	   	    self.received_step_hashes
	                      .write()
	                      .insert(received_step_key, new_hash);
	*/
}

/// Phase 3 verification. Check block information against parent. Returns either a null `Ok` or a general error detailing the problem with import.
//nolint
func (c *AuRa) VerifyFamily(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
	step, err := headerStep(header)
	if err != nil {
		return err
	}
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	parentStep, err := headerStep(parent)
	if err != nil {
		return err
	}
	validators, setNumber, err := c.epochSet(chain, header)
	if err != nil {
		return err
	}

	// Ensure header is from the step after parent.
	if step == parentStep ||
		(header.Number.Uint64() >= c.cfg.ValidateStepTransition && step <= parentStep) {
		log.Debug("[engine] Multiple blocks proposed for step", "num", parentStep)
		_ = setNumber
		/*
			self.validators.report_malicious(
				header.author(),
				set_number,
				header.number(),
				Default::default(),
			);
			Err(EngineError::DoubleVote(*header.author()))?;
		*/
		return fmt.Errorf("double vote: %x", header.Coinbase)
	}

	// Report malice if the validator produced other sibling blocks in the same step.
	if !c.hasReceivedStepHashes(step, header.Coinbase, header.Hash()) {
		/*
		   trace!(target: "engine", "Validator {} produced sibling blocks in the same step", header.author());
		   self.validators.report_malicious(
		       header.author(),
		       set_number,
		       header.number(),
		       Default::default(),
		   );
		*/
	} else {
		c.insertReceivedStepHashes(step, header.Coinbase, header.Hash())
	}

	// Remove hash records older than two full rounds of steps (picked as a reasonable trade-off between
	// memory consumption and fault-tolerance).
	cnt, err := count(validators, parent.Hash())
	if err != nil {
		return err
	}
	siblingMaliceDetectionPeriod := 2 * cnt
	oldestStep := uint64(0) //  let oldest_step = parent_step.saturating_sub(sibling_malice_detection_period);
	if parentStep > siblingMaliceDetectionPeriod {
		oldestStep = parentStep - siblingMaliceDetectionPeriod
	}
	//nolint
	if oldestStep > 0 {
		/*
		   let mut rsh = self.received_step_hashes.write();
		   let new_rsh = rsh.split_off(&(oldest_step, Address::zero()));
		   *rsh = new_rsh;
		*/
	}

	emptyStepLen := uint64(0)
	//self.report_skipped(header, step, parent_step, &*validators, set_number);

	/*
	   // If empty step messages are enabled we will validate the messages in the seal, missing messages are not
	   // reported as there's no way to tell whether the empty step message was never sent or simply not included.
	   let empty_steps_len = if header.number() >= self.empty_steps_transition {
	       let validate_empty_steps = || -> Result<usize, Error> {
	           let strict_empty_steps = header.number() >= self.strict_empty_steps_transition;
	           let empty_steps = header_empty_steps(header)?;
	           let empty_steps_len = empty_steps.len();
	           let mut prev_empty_step = 0;

	           for empty_step in empty_steps {
	               if empty_step.step <= parent_step || empty_step.step >= step {
	                   Err(EngineError::InsufficientProof(format!(
	                       "empty step proof for invalid step: {:?}",
	                       empty_step.step
	                   )))?;
	               }

	               if empty_step.parent_hash != *header.parent_hash() {
	                   Err(EngineError::InsufficientProof(format!(
	                       "empty step proof for invalid parent hash: {:?}",
	                       empty_step.parent_hash
	                   )))?;
	               }

	               if !empty_step.verify(&*validators).unwrap_or(false) {
	                   Err(EngineError::InsufficientProof(format!(
	                       "invalid empty step proof: {:?}",
	                       empty_step
	                   )))?;
	               }

	               if strict_empty_steps {
	                   if empty_step.step <= prev_empty_step {
	                       Err(EngineError::InsufficientProof(format!(
	                           "{} empty step: {:?}",
	                           if empty_step.step == prev_empty_step {
	                               "duplicate"
	                           } else {
	                               "unordered"
	                           },
	                           empty_step
	                       )))?;
	                   }

	                   prev_empty_step = empty_step.step;
	               }
	           }

	           Ok(empty_steps_len)
	       };

	       match validate_empty_steps() {
	           Ok(len) => len,
	           Err(err) => {
	               trace!(
	                   target: "engine",
	                   "Reporting benign misbehaviour (cause: invalid empty steps) \
	                   at block #{}, epoch set number {}. Own address: {}",
	                   header.number(), set_number, self.address().unwrap_or_default()
	               );
	               self.validators
	                   .report_benign(header.author(), set_number, header.number());
	               return Err(err);
	           }
	       }
	   } else {
	       self.report_skipped(header, step, parent_step, &*validators, set_number);

	       0
	   };
	*/
	if header.Number.Uint64() >= c.cfg.ValidateScoreTransition {
		expectedDifficulty := calculateScore(parentStep, step, emptyStepLen)
		if header.Difficulty.Cmp(expectedDifficulty.ToBig()) != 0 {
			return fmt.Errorf("invlid difficulty: expect=%s, found=%s\n", expectedDifficulty, header.Difficulty)
		}
	}
	return nil
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *AuRa) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, _ []bool) error {
	return nil
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *AuRa) VerifyUncles(chain consensus.ChainReader, header *types.Header, uncles []*types.Header) error {
	return nil
	//if len(uncles) > 0 {
	//	return errors.New("uncles not allowed")
	//}
	//return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *AuRa) VerifySeal(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
	//snap, err := c.Snapshot(chain, header.Number.Uint64(), header.Hash(), nil)
	//if err != nil {
	//	return err
	//}
	//return c.verifySeal(chain, header, snap)
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *AuRa) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	return nil
	/// If the block isn't a checkpoint, cast a random vote (good enough for now)
	//header.Coinbase = common.Address{}
	//header.Nonce = types.BlockNonce{}
	//
	//number := header.Number.Uint64()
	/// Assemble the voting snapshot to check which votes make sense
	//snap, err := c.Snapshot(chain, number-1, header.ParentHash, nil)
	//if err != nil {
	//	return err
	//}
	//if number%c.config.Epoch != 0 {
	//	c.lock.RLock()
	//
	//	// Gather all the proposals that make sense voting on
	//	addresses := make([]common.Address, 0, len(c.proposals))
	//	for address, authorize := range c.proposals {
	//		if snap.validVote(address, authorize) {
	//			addresses = append(addresses, address)
	//		}
	//	}
	//	// If there's pending proposals, cast a vote on them
	//	if len(addresses) > 0 {
	//		header.Coinbase = addresses[rand.Intn(len(addresses))]
	//		if c.proposals[header.Coinbase] {
	//			copy(header.Nonce[:], NonceAuthVote)
	//		} else {
	//			copy(header.Nonce[:], nonceDropVote)
	//		}
	//	}
	//	c.lock.RUnlock()
	//}
	/// Set the correct difficulty
	//header.Difficulty = calcDifficulty(snap, c.signer)
	//
	/// Ensure the extra data has all its components
	//if len(header.Extra) < ExtraVanity {
	//	header.Extra = append(header.Extra, bytes.Repeat([]byte{0x00}, ExtraVanity-len(header.Extra))...)
	//}
	//header.Extra = header.Extra[:ExtraVanity]
	//
	//if number%c.config.Epoch == 0 {
	//	for _, signer := range snap.GetSigners() {
	//		header.Extra = append(header.Extra, signer[:]...)
	//	}
	//}
	//header.Extra = append(header.Extra, make([]byte, ExtraSeal)...)
	//
	/// Mix digest is reserved for now, set to empty
	//header.MixDigest = common.Hash{}
	//
	/// Ensure the timestamp has the correct delay
	//parent := chain.GetHeader(header.ParentHash, number-1)
	//if parent == nil {
	//	return consensus.ErrUnknownAncestor
	//}
	//header.Time = parent.Time + c.config.Period
	//
	//now := uint64(time.Now().Unix())
	//if header.Time < now {
	//	header.Time = now
	//}
	//
	//return nil
}

func (c *AuRa) Initialize(cc *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
	isEpochBegin := header.Number.Uint64() == 1
	if !isEpochBegin {
		return
	}
	err := c.cfg.Validators.onEpochBegin(isEpochBegin, header, syscall)
	if err != nil {
		log.Warn("aura initialize block: on epoch begin", "err", err)
	}
}

func (c *AuRa) Finalize(cc *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, syscall consensus.SystemCall) {
	// accumulateRewards retreives rewards for a block and applies them to the coinbase accounts for miner and uncle miners
	beneficiaries, _, rewards, err := AccumulateRewards(cc, c, header, uncles, syscall)
	if err != nil {
		log.Error("accumulateRewards", "err", err)
		return
	}
	for i := range beneficiaries {
		state.AddBalance(beneficiaries[i], rewards[i])
	}
}

// FinalizeAndAssemble implements consensus.Engine
func (c *AuRa) FinalizeAndAssemble(chainConfig *params.ChainConfig, header *types.Header, state *state.IntraBlockState, txs []types.Transaction, uncles []*types.Header, receipts []*types.Receipt, syscall consensus.SystemCall) (*types.Block, error) {
	c.Finalize(chainConfig, header, state, txs, uncles, syscall)

	// Assemble and return the final block for sealing
	return types.NewBlock(header, txs, uncles, receipts), nil
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *AuRa) Authorize(signer common.Address, signFn clique.SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	//c.signer = signer
	//c.signFn = signFn
}

func (c *AuRa) GenesisEpochData(header *types.Header, caller Call) ([]byte, error) {
	proof, err := c.cfg.Validators.genesisEpochData(header, caller)
	if err != nil {
		return nil, err
	}
	return combineProofs(0, proof, []byte{}), nil
}
func combineProofs(signalNumber uint64, setProof []byte, finalityProof []byte) []byte {
	return common.FromHex("0xf91a8c80b91a87f91a84f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f91871b853f851808080a07bb75cabebdcbd1dbb4331054636d0c6d7a2b08483b9e04df057395a7434c9e080808080808080a0e61e567237b49c44d8f906ceea49027260b4010c10a547b38d8b131b9d3b6f848080808080b86bf869a02080c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312ab846f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470b86bf869a033aa5d69545785694b808840be50c182dad2ec3636dfccbe6572fb69828742c0b846f8440101a0663ce0d171e545a26aa67e4ca66f72ba96bb48287dbcc03beea282867f80d44ba01f0e7726926cb43c03a0abf48197dba78522ec8ba1b158e2aa30da7d2a2c6f9ea3e2a02052222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f01b914c26060604052600436106100fc576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303aca79214610101578063108552691461016457806340a141ff1461019d57806340c9cdeb146101d65780634110a489146101ff57806345199e0a1461025757806349285b58146102c15780634d238c8e14610316578063752862111461034f578063900eb5a8146103645780639a573786146103c7578063a26a47d21461041c578063ae4b1b5b14610449578063b3f05b971461049e578063b7ab4db5146104cb578063d3e848f114610535578063fa81b2001461058a578063facd743b146105df575b600080fd5b341561010c57600080fd5b6101226004808035906020019091905050610630565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561016f57600080fd5b61019b600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190505061066f565b005b34156101a857600080fd5b6101d4600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610807565b005b34156101e157600080fd5b6101e9610bb7565b6040518082815260200191505060405180910390f35b341561020a57600080fd5b610236600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610bbd565b60405180831515151581526020018281526020019250505060405180910390f35b341561026257600080fd5b61026a610bee565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156102ad578082015181840152602081019050610292565b505050509050019250505060405180910390f35b34156102cc57600080fd5b6102d4610c82565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561032157600080fd5b61034d600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610d32565b005b341561035a57600080fd5b610362610fcc565b005b341561036f57600080fd5b61038560048080359060200190919050506110fc565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156103d257600080fd5b6103da61113b565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561042757600080fd5b61042f6111eb565b604051808215151515815260200191505060405180910390f35b341561045457600080fd5b61045c6111fe565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156104a957600080fd5b6104b1611224565b604051808215151515815260200191505060405180910390f35b34156104d657600080fd5b6104de611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b83811015610521578082015181840152602081019050610506565b505050509050019250505060405180910390f35b341561054057600080fd5b6105486112cb565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561059557600080fd5b61059d6112f1565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156105ea57600080fd5b610616600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050611317565b604051808215151515815260200191505060405180910390f35b60078181548110151561063f57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156106cb57600080fd5b600460019054906101000a900460ff161515156106e757600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff161415151561072357600080fd5b80600a60006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055506001600460016101000a81548160ff0219169083151502179055507f600bcf04a13e752d1e3670a5a9f1c21177ca2a93c6f5391d4f1298d098097c22600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a150565b600080600061081461113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff1614151561084d57600080fd5b83600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff1615156108a957600080fd5b600960008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101549350600160078054905003925060078381548110151561090857fe5b906000526020600020900160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1691508160078581548110151561094657fe5b906000526020600020900160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555083600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506007838154811015156109e557fe5b906000526020600020900160006101000a81549073ffffffffffffffffffffffffffffffffffffffff02191690556000600780549050111515610a2757600080fd5b6007805480919060019003610a3c9190611370565b506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160006101000a81548160ff0219169083151502179055506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610ba257602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610b58575b50509250505060405180910390a25050505050565b60085481565b60096020528060005260406000206000915090508060000160009054906101000a900460ff16908060010154905082565b610bf661139c565b6007805480602002602001604051908101604052809291908181526020018280548015610c7857602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610c2e575b5050505050905090565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166349285b586000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b1515610d1257600080fd5b6102c65a03f11515610d2357600080fd5b50505060405180519050905090565b610d3a61113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141515610d7357600080fd5b80600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff16151515610dd057600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1614151515610e0c57600080fd5b6040805190810160405280600115158152602001600780549050815250600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008201518160000160006101000a81548160ff0219169083151502179055506020820151816001015590505060078054806001018281610ea991906113b0565b9160005260206000209001600084909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610fba57602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610f70575b50509250505060405180910390a25050565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480156110365750600460009054906101000a900460ff16155b151561104157600080fd5b6001600460006101000a81548160ff0219169083151502179055506007600690805461106e9291906113dc565b506006805490506008819055507f8564cd629b15f47dc310d45bcbfc9bcf5420b0d51bf0659a16c67f91d27632536110a4611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156110e75780820151818401526020810190506110cc565b505050509050019250505060405180910390a1565b60068181548110151561110b57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16639a5737866000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b15156111cb57600080fd5b6102c65a03f115156111dc57600080fd5b50505060405180519050905090565b600460019054906101000a900460ff1681565b600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460009054906101000a900460ff1681565b61123f61139c565b60068054806020026020016040519081016040528092919081815260200182805480156112c157602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311611277575b5050505050905090565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600960008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff169050919050565b81548183558181151161139757818360005260206000209182019101611396919061142e565b5b505050565b602060405190810160405280600081525090565b8154818355818115116113d7578183600052602060002091820191016113d6919061142e565b5b505050565b82805482825590600052602060002090810192821561141d5760005260206000209182015b8281111561141c578254825591600101919060010190611401565b5b50905061142a9190611453565b5090565b61145091905b8082111561144c576000816000905550600101611434565b5090565b90565b61149391905b8082111561148f57600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff021916905550600101611459565b5090565b905600a165627a7a7230582036ea35935c8246b68074adece2eab70c40e69a0193c08a6277ce06e5b25188510029b8f3f8f1a08023c0d95fc2364e0bf7593f5ff32e1db8ef9f4b41c0bd474eae62d1af896e99808080a0b47b4f0b3e73b5edc8f9a9da1cbcfed562eb06bf54619b6aefeadebf5b3604c280a0da6ec08940a924cb08c947dd56cdb40076b29a6f0ea4dba4e2d02d9a9a72431b80a030cc4138c9e74b6cf79d624b4b5612c0fd888e91f55316cfee7d1694e1a90c0b80a0c5d54b915b56a888eee4e6eeb3141e778f9b674d1d322962eed900f02c29990aa017256b36ef47f907c6b1378a2636942ce894c17075e56fc054d4283f6846659e808080a03340bbaeafcda3a8672eb83099231dbbfab8dae02a1e8ec2f7180538fac207e080b853f851808080a0a87d9bb950836582673aa0eecc0ff64aac607870637a2dd2012b8b1b31981f698080a08da6d5c36a404670c553a2c9052df7cd604f04e3863c4c7b9e0027bfd54206d680808080808080808080b838f7a03868bdfa8727775661e4ccf117824a175a33f8703d728c04488fbfffcafda9f99594e8ddc5c7a2d2f0d7a9798459c0104fdf5e987acab8d3f8d1a0dc277c93a9f9dcee99aac9b8ba3cfa4c51821998522469c37715644e8fbac0bfa0ab8cdb808c8303bb61fb48e276217be9770fa83ecf3f90f2234d558885f5abf1808080a0fe137c3a474fbde41d89a59dd76da4c55bf696b86d3af64a55632f76cf30786780808080a06301b39b2ea8a44df8b0356120db64b788e71f52e1d7a6309d0d2e5b86fee7cb80a0da5d8b08dea0c5a4799c0f44d8a24d7cdf209f9b7a5588c1ecafb5361f6b9f07a01b7779e149cadf24d4ffb77ca7e11314b8db7097e4d70b2a173493153ca2e5a080808080")
	/*
	   let mut stream = RlpStream::new_list(3);
	   stream
	       .append(&signal_number)
	       .append(&set_proof)
	       .append(&finality_proof);
	   stream.out()
	*/
}

func (c *AuRa) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	return nil
	//header := block.Header()
	//
	/// Sealing the genesis block is not supported
	//number := header.Number.Uint64()
	//if number == 0 {
	//	return errUnknownBlock
	//}
	/// For 0-period chains, refuse to seal empty blocks (no reward but would spin sealing)
	//if c.config.Period == 0 && len(block.Transactions()) == 0 {
	//	log.Info("Sealing paused, waiting for transactions")
	//	return nil
	//}
	/// Don't hold the signer fields for the entire sealing procedure
	//c.lock.RLock()
	//signer, signFn := c.signer, c.signFn
	//c.lock.RUnlock()
	//
	/// Bail out if we're unauthorized to sign a block
	//snap, err := c.Snapshot(chain, number-1, header.ParentHash, nil)
	//if err != nil {
	//	return err
	//}
	//if _, authorized := snap.Signers[signer]; !authorized {
	//	return ErrUnauthorizedSigner
	//}
	/// If we're amongst the recent signers, wait for the next block
	//for seen, recent := range snap.Recents {
	//	if recent == signer {
	//		// Signer is among RecentsRLP, only wait if the current block doesn't shift it out
	//		if limit := uint64(len(snap.Signers)/2 + 1); number < limit || seen > number-limit {
	//			log.Info("Signed recently, must wait for others")
	//			return nil
	//		}
	//	}
	//}
	/// Sweet, the protocol permits us to sign the block, wait for our time
	//delay := time.Unix(int64(header.Time), 0).Sub(time.Now()) // nolint: gosimple
	//if header.Difficulty.Cmp(diffNoTurn) == 0 {
	//	// It's not our turn explicitly to sign, delay it a bit
	//	wiggle := time.Duration(len(snap.Signers)/2+1) * wiggleTime
	//	delay += time.Duration(rand.Int63n(int64(wiggle)))
	//
	//	log.Trace("Out-of-turn signing requested", "wiggle", common.PrettyDuration(wiggle))
	//}
	/// Sign all the things!
	//sighash, err := signFn(signer, accounts.MimetypeClique, CliqueRLP(header))
	//if err != nil {
	//	return err
	//}
	//copy(header.Extra[len(header.Extra)-ExtraSeal:], sighash)
	/// Wait until sealing is terminated or delay timeout.
	//log.Trace("Waiting for slot to sign and propagate", "delay", common.PrettyDuration(delay))
	//go func() {
	//	select {
	//	case <-stop:
	//		return
	//	case <-time.After(delay):
	//	}
	//
	//	select {
	//	case results <- block.WithSeal(header):
	//	default:
	//		log.Warn("Sealing result is not read by miner", "sealhash", SealHash(header))
	//	}
	//}()
	//
	//return nil
}

func stepProposer(validators ValidatorSet, blockHash common.Hash, step uint64) (common.Address, error) {
	c, err := validators.defaultCaller(blockHash)
	if err != nil {
		return common.Address{}, err
	}
	return validators.getWithCaller(blockHash, uint(step), c)
}

// GenerateSeal - Attempt to seal the block internally.
//
// This operation is synchronous and may (quite reasonably) not be available, in which case
// `Seal::None` will be returned.
func (c *AuRa) GenerateSeal(chain consensus.ChainHeaderReader, current, parent *types.Header) []rlp.RawValue {
	// first check to avoid generating signature most of the time
	// (but there's still a race to the `compare_exchange`)
	if !c.step.canPropose.Load() {
		log.Trace("[engine] Aborting seal generation. Can't propose.")
		return nil
	}
	parentStep, err := headerStep(parent)
	if err != nil {
		panic(err)
	}
	step := c.step.inner.inner.Load()

	// filter messages from old and future steps and different parents
	expectedDiff := calculateScore(parentStep, step, 0)
	if current.Difficulty.Cmp(expectedDiff.ToBig()) != 0 {
		log.Debug(fmt.Sprintf("[engine] Aborting seal generation. The step or empty_steps have changed in the meantime. %d != %d", current.Difficulty, expectedDiff))
		return nil
	}

	if parentStep > step {
		log.Warn(fmt.Sprintf("[engine] Aborting seal generation for invalid step: %d > %d", parentStep, step))
		return nil
	}

	validators, setNumber, err := c.epochSet(chain, current)
	if err != nil {
		log.Warn("[engine] Unable to generate seal", "err", err)
		return nil
	}

	stepProposerAddr, err := stepProposer(validators, current.ParentHash, step)
	if err != nil {
		log.Warn("[engine] Unable to get stepProposer", "err", err)
		return nil
	}
	if stepProposerAddr != current.Coinbase {
		return nil
	}

	// this is guarded against by `can_propose` unless the block was signed
	// on the same step (implies same key) and on a different node.
	if parentStep == step {
		log.Warn("Attempted to seal block on the same step as parent. Is this authority sealing with more than one node?")
		return nil
	}

	_ = setNumber
	/*
		signature, err := c.sign(current.bareHash())
			if err != nil {
				log.Warn("[engine] generate_seal: FAIL: Accounts secret key unavailable.", "err", err)
				return nil
			}
	*/

	/*
		  // only issue the seal if we were the first to reach the compare_exchange.
		  if self
			  .step
			  .can_propose
			  .compare_exchange(true, false, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
			  .is_ok()
		  {
			  // we can drop all accumulated empty step messages that are
			  // older than the parent step since we're including them in
			  // the seal
			  self.clear_empty_steps(parent_step);

			  // report any skipped primaries between the parent block and
			  // the block we're sealing, unless we have empty steps enabled
			  if header.number() < self.empty_steps_transition {
				  self.report_skipped(header, step, parent_step, &*validators, set_number);
			  }

			  let mut fields =
				  vec![encode(&step), encode(&(H520::from(signature).as_bytes()))];

			  if let Some(empty_steps_rlp) = empty_steps_rlp {
				  fields.push(empty_steps_rlp);
			  }

			  return Seal::Regular(fields);
		  }
	*/
	return nil
}

// epochSet fetch correct validator set for epoch at header, taking into account
// finality of previous transitions.
func (c *AuRa) epochSet(chain consensus.ChainHeaderReader, h *types.Header) (ValidatorSet, uint64, error) {
	if c.cfg.ImmediateTransitions {
		return c.cfg.Validators, h.Number.Uint64(), nil
	}

	finalityChecker, epochTransitionNumber, ok := c.EpochManager.zoomToAfter(chain, c.cfg.Validators, h.ParentHash)
	if !ok {
		return nil, 0, fmt.Errorf("unable to zoomToAfter to epoch")
	}
	return finalityChecker.signers, epochTransitionNumber, nil
}

//nolint
func headerStep(current *types.Header) (val uint64, err error) {
	if len(current.Seal) < 1 {
		panic("was either checked with verify_block_basic or is genesis; has 2 fields; qed (Make sure the spec file has a correct genesis seal)")
	}
	err = rlp.Decode(bytes.NewReader(current.Seal[0]), &val)
	if err != nil {
		return val, err
	}
	return val, err
}

func (c *AuRa) CalcDifficulty(chain consensus.ChainHeaderReader, time, parentTime uint64, parentDifficulty *big.Int, parentNumber uint64, parentHash, parentUncleHash common.Hash, parentSeal []rlp.RawValue) *big.Int {
	var parentStep uint64
	err := rlp.Decode(bytes.NewReader(parentSeal[0]), &parentStep)
	if err != nil {
		panic(err)
	}
	currentStep := c.step.inner.inner.Load()
	currentEmptyStepsLen := 0
	return calculateScore(parentStep, currentStep, uint64(currentEmptyStepsLen)).ToBig()

	/* TODO: do I need gasLimit override logic here ?
	if let Some(gas_limit) = self.gas_limit_override(header) {
		trace!(target: "engine", "Setting gas limit to {} for block {}.", gas_limit, header.number());
		let parent_gas_limit = *parent.gas_limit();
		header.set_gas_limit(gas_limit);
		if parent_gas_limit != gas_limit {
			info!(target: "engine", "Block gas limit was changed from {} to {}.", parent_gas_limit, gas_limit);
		}
	}
	*/
}

// calculateScore - analog of PoW difficulty:
//    sqrt(U256::max_value()) + parent_step - current_step + current_empty_steps
func calculateScore(parentStep, currentStep, currentEmptySteps uint64) *uint256.Int {
	maxU128 := uint256.NewInt(0).SetAllOne()
	maxU128 = maxU128.Rsh(maxU128, 128)
	res := maxU128.Add(maxU128, uint256.NewInt(parentStep))
	res = res.Sub(res, uint256.NewInt(currentStep))
	res = res.Add(res, uint256.NewInt(currentEmptySteps))
	return res
}

func (c *AuRa) SealHash(header *types.Header) common.Hash {
	return clique.SealHash(header)
}

// Close implements consensus.Engine. It's a noop for clique as there are no background threads.
func (c *AuRa) Close() error {
	common.SafeClose(c.exitCh)
	return nil
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c *AuRa) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return []rpc.API{
		//{
		//Namespace: "clique",
		//Version:   "1.0",
		//Service:   &API{chain: chain, clique: c},
		//Public:    false,
		//}
	}
}

//nolint
func (c *AuRa) emptySteps(fromStep, toStep uint64, parentHash common.Hash) []EmptyStep {
	from := EmptyStep{step: fromStep + 1, parentHash: parentHash}
	to := EmptyStep{step: toStep}
	res := []EmptyStep{}
	if to.LessOrEqual(&from) {
		return res
	}

	c.EmptyStepsSet.Sort()
	c.EmptyStepsSet.ForEach(func(i int, step *EmptyStep) {
		if step.Less(&from) || (&to).Less(step) {
			return
		}
		if step.parentHash != parentHash {
			return
		}
		res = append(res, *step)
	})
	return res
}

// AccumulateRewards returns rewards for a given block. The mining reward consists
// of the static blockReward plus a reward for each included uncle (if any). Individual
// uncle rewards are also returned in an array.
func AccumulateRewards(_ *params.ChainConfig, aura *AuRa, header *types.Header, _ []*types.Header, syscall consensus.SystemCall) (beneficiaries []common.Address, rewardKind []aurainterfaces.RewardKind, rewards []*uint256.Int, err error) {
	beneficiaries = append(beneficiaries, header.Coinbase)
	rewardKind = append(rewardKind, aurainterfaces.RewardAuthor)

	var rewardContractAddress BlockRewardContract
	var foundContract bool
	for _, c := range aura.cfg.BlockRewardContractTransitions {
		if c.blockNum > header.Number.Uint64() {
			break
		}
		foundContract = true
		rewardContractAddress = c
	}
	if foundContract {
		beneficiaries, rewards = callBlockRewardAbi(rewardContractAddress.address, syscall, beneficiaries, rewardKind)
		rewardKind = rewardKind[:len(beneficiaries)]
		for i := 0; i < len(rewardKind); i++ {
			rewardKind[i] = aurainterfaces.RewardExternal
		}
	} else {
		// block_reward.iter.rev().find(|&(block, _)| *block <= number)
		var reward BlockReward
		var found bool
		for i := range aura.cfg.BlockReward {
			if aura.cfg.BlockReward[i].blockNum > header.Number.Uint64() {
				break
			}
			found = true
			reward = aura.cfg.BlockReward[i]
		}
		if !found {
			panic("Current block's reward is not found; this indicates a chain config error")
		}

		for range beneficiaries {
			rewards = append(rewards, reward.amount)
		}
	}

	//err = aura.cfg.Validators.onCloseBlock(header, aura.OurSigningAddress)
	//if err != nil {
	//	return
	//}
	return
}

func callBlockRewardAbi(contractAddr common.Address, syscall consensus.SystemCall, beneficiaries []common.Address, rewardKind []aurainterfaces.RewardKind) ([]common.Address, []*uint256.Int) {
	castedKind := make([]uint16, len(rewardKind))
	for i := range rewardKind {
		castedKind[i] = uint16(rewardKind[i])
	}
	packed, err := blockRewardAbi().Pack("reward", beneficiaries, castedKind)
	if err != nil {
		panic(err)
	}
	out, err := syscall(contractAddr, packed)
	if err != nil {
		panic(err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	res, err := blockRewardAbi().Unpack("reward", out)
	if err != nil {
		panic(err)
	}
	_ = res[0]
	_ = res[1]
	return nil, nil
}

func blockRewardAbi() abi.ABI {
	a, err := abi.JSON(bytes.NewReader(contracts.BlockReward))
	if err != nil {
		panic(err)
	}
	return a
}

// An empty step message that is included in a seal, the only difference is that it doesn't include
// the `parent_hash` in order to save space. The included signature is of the original empty step
// message, which can be reconstructed by using the parent hash of the block in which this sealed
// empty message is inc    luded.
//nolint
type SealedEmptyStep struct {
	signature []byte // H520
	step      uint64
}

/*
// extracts the empty steps from the header seal. should only be called when there are 3 fields in the seal
// (i.e. header.number() >= self.empty_steps_transition).
func headerEmptySteps(header *types.Header) ([]EmptyStep, error) {
	s := headerEmptyStepsRaw(header)
	sealedSteps := []SealedEmptyStep{}
	err := rlp.DecodeBytes(s, &sealedSteps)
	if err != nil {
		return nil, err
	}
	steps := make([]EmptyStep, len(sealedSteps))
	for i := range sealedSteps {
		steps[i] = newEmptyStepFromSealed(sealedSteps[i], header.ParentHash)
	}
	return steps, nil
}

func newEmptyStepFromSealed(step SealedEmptyStep, parentHash common.Hash) EmptyStep {
	return EmptyStep{
		signature:  step.signature,
		step:       step.step,
		parentHash: parentHash,
	}
}

// extracts the raw empty steps vec from the header seal. should only be called when there are 3 fields in the seal
// (i.e. header.number() >= self.empty_steps_transition)
func headerEmptyStepsRaw(header *types.Header) []byte {
	if len(header.Seal) < 3 {
		panic("was checked with verify_block_basic; has 3 fields; qed")
	}
	return header.Seal[2]
}
*/

// A message broadcast by authorities when it's their turn to seal a block but there are no
// transactions. Other authorities accumulate these messages and later include them in the seal as
// proof.
//
// An empty step message is created _instead of_ a block if there are no pending transactions.
// It cannot itself be a parent, and `parent_hash` always points to the most recent block. E.g.:
// * Validator A creates block `bA`.
// * Validator B has no pending transactions, so it signs an empty step message `mB`
//   instead whose hash points to block `bA`.
// * Validator C also has no pending transactions, so it also signs an empty step message `mC`
//   instead whose hash points to block `bA`.
// * Validator D creates block `bD`. The parent is block `bA`, and the header includes `mB` and `mC`.
type EmptyStep struct {
	// The signature of the other two fields, by the message's author.
	signature []byte // H520
	// This message's step number.
	step uint64
	// The hash of the most recent block.
	parentHash common.Hash //     H256
}

func (s *EmptyStep) Less(other *EmptyStep) bool {
	if s.step < other.step {
		return true
	}
	if bytes.Compare(s.parentHash[:], other.parentHash[:]) < 0 {
		return true
	}
	if bytes.Compare(s.signature, other.signature) < 0 {
		return true
	}
	return false
}
func (s *EmptyStep) LessOrEqual(other *EmptyStep) bool {
	if s.step <= other.step {
		return true
	}
	if bytes.Compare(s.parentHash[:], other.parentHash[:]) <= 0 {
		return true
	}
	if bytes.Compare(s.signature, other.signature) <= 0 {
		return true
	}
	return false
}

// Returns `true` if the message has a valid signature by the expected proposer in the message's step.
func (s *EmptyStep) verify(validators ValidatorSet) (bool, error) { //nolint
	//sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	//if err != nil {
	//	return false, err
	//}
	//message := crypto.Keccak256(sRlp)

	/*
		let correct_proposer = step_proposer(validators, &self.parent_hash, self.step);

		publickey::verify_address(&correct_proposer, &self.signature.into(), &message)
		.map_err(|e| e.into())
	*/
	return true, nil
}

//nolint
func (s *EmptyStep) author() (common.Address, error) {
	sRlp, err := EmptyStepRlp(s.step, s.parentHash)
	if err != nil {
		return common.Address{}, err
	}
	message := crypto.Keccak256(sRlp)
	public, err := secp256k1.RecoverPubkey(message, s.signature)
	if err != nil {
		return common.Address{}, err
	}
	ecdsa, err := crypto.UnmarshalPubkey(public)
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*ecdsa), nil
}

type EmptyStepSet struct {
	lock sync.Mutex
	list []*EmptyStep
}

func (s *EmptyStepSet) Less(i, j int) bool { return s.list[i].Less(s.list[j]) }
func (s *EmptyStepSet) Swap(i, j int)      { s.list[i], s.list[j] = s.list[j], s.list[i] }
func (s *EmptyStepSet) Len() int           { return len(s.list) }

func (s *EmptyStepSet) Sort() {
	s.lock.Lock()
	defer s.lock.Unlock()
	sort.Stable(s)
}

func (s *EmptyStepSet) ForEach(f func(int, *EmptyStep)) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i, el := range s.list {
		f(i, el)
	}
}

func EmptyStepFullRlp(signature []byte, emptyStepRlp []byte) ([]byte, error) {
	type A struct {
		s []byte
		r []byte
	}

	return rlp.EncodeToBytes(A{s: signature, r: emptyStepRlp})
}

func EmptyStepRlp(step uint64, parentHash common.Hash) ([]byte, error) {
	type A struct {
		s uint64
		h common.Hash
	}
	return rlp.EncodeToBytes(A{s: step, h: parentHash})
}

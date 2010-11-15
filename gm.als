module gm

sig Member {
	left : one Member,
	right : one Member,
	member_reduces_to : lone Member,
	process : some Process
}{
	this in this.^@right // enforce a ring
	no (this & this.^@member_reduces_to) // no cycles
	lone this.~@member_reduces_to // at most one preceeding reduction
}

fact { right = ~left }

sig View {
	members : set Member,
	next : lone View
}{
	no (this & this.^@next) // no cycles
	lone this.~@next // at most one preceeding view
	no members & members.^member_reduces_to // members do not reduce within the same view
}

fact { one v : View | no v.next } // only one last View
fact { one v : View | no v.~next } // only one first View
// Every member exists in exactly one view
fact { all m : Member | one m.~members }
// Within a view, we have one ring only...
fact { all v : View, m, m' : Member | (m in v.members and m' in v.members) => m' in m.*right }
// ...and that ring is entirely contained within members
fact { all v : View, m : Member | m in v.members => v.members = m.*right }
// if a member reduces to something then that something must be in the next view
fact { all v : View, m : Member |
		(m in v.members and some m.member_reduces_to) => m.member_reduces_to in v.next.members }

pred add_member [v, v' : View] {
	one v'.members - v.members.member_reduces_to
	v.members = v.members & v'.members.~member_reduces_to
}

pred remove_member [v, v' : View] {
	one v.members - v'.members.~member_reduces_to
	v'.members = v'.members & v.members.member_reduces_to
}

pred view_change [v, v' : View] {
	add_member[v, v'] <=> not remove_member[v, v']
}

fact { all v, v' : View | v' = v.next => view_change[v, v'] } // there must be a reduction linking consecutive views

check membership_changes_by_one {
	all v, v' : View | v' = v.next => (#v'.members = #v.members +1 or #v'.members = #v.members -1)
} for 10

sig Message {
	from : one Process,
	to : one Process,
	payload : one Payload
}{
	no (from & to)
}

// each process can be involved in at most one send xor one receive
fact { all p : Process | lone (p.~from + p.~to) }

// take a message between two processes:
// there is no intersection between the past of the message's from or to with any future state reachable from this or any subsequent message reachable from this.
// Enforces asynchronous communication that obeys temporal mechanics
fact { all m : Message | no ((m.from + m.to).^~process_reduces_to) & m.from.^(*process_reduces_to.~from.to) }

sig Process {
	process_reduces_to : lone Process,
	process_state : one ProcessState
}{
	no (this & this.^@process_reduces_to) // no cycles
	lone this.~@process_reduces_to // at most one predecessor	
}

// the first process can't do anything, and has a blank state
fact { all p : Process | (no p.~process_reduces_to) => (no (p.~from + p.~to) and no p.process_state.pending_acks )}

check message_goes_forward {
	all m : Message | no (m.from.*~process_reduces_to & m.to.*process_reduces_to)
} for 7

check messages_receive_order_matches_send_order {
	all m, m' : Message |
		((m'.from in m.from.*process_reduces_to) and
		some (m.to.*process_reduces_to & m'.to.*process_reduces_to)) =>
			m'.to in m.to.*process_reduces_to
} for 7

// be as loose as possible in specifying the overlap, to allow other facts to enforce ordering
pred can_send_to_self {
	all m : Message | some (m.to.*process_reduces_to & m.from.*process_reduces_to)
	all p : Process | some (p.~from + p.~to)
}
run can_send_to_self for exactly 1 Member, 1 View, 3 Message, 6 Process

// limit the reduction of a process to the reduction of a member
fact {
	all m : Member, p : Process |
		p in m.process => p.*process_reduces_to in m.(*member_reduces_to).process
}
// all processes can be reached through members
fact { Process = Member.(*member_reduces_to).process }
// a process is owned by at most one member
fact { all p : Process | lone p.~process }
// all the processes a member owns form a single chain of reduction
fact { all p, p' : Process, m : Member | (p in m.process and p' in m.process) => (p' in p.*process_reduces_to or p in p'.*process_reduces_to) }
// if a member reduces then it must continue reducing the same process
fact { all m : Member, p : Process | (p in m.process and some m.member_reduces_to) => (m.member_reduces_to.process in p.*process_reduces_to) }
// messages must be received in the current or any future view
fact { all m : Message | m.to.~process.~members in m.from.~process.~members.*next }
// a message cannot be sent to a process which does not have a forefather in the sender's view (knowledge of peers is limited to the present and past)
// READ: "There is a non-empty set formed by the intersection of [the (reflexive) ancestors of the destination process of the message] with [the processes in the sender's view]"
fact { all m : Message | some (m.to.*~process_reduces_to & m.from.~process.~members.members.process) }

pred process_spawn [p, p' : Process] {
	p' in p.*process_reduces_to and no p.~process_reduces_to
}

abstract sig Payload {}

sig Pub extends Payload {}

sig Ack extends Payload {
	pub : one Pub
}{
	one pub.~@pub
}

sig ProcessState {
	pending_acks : Process -> set Pub
}{
	all p : Process | some p.pending_acks => no p.~process_reduces_to
}

// every processstate is linked to from a process
fact { ProcessState = Process.process_state }
// every pub is linked to from a message
fact { Pub in Message.payload }
// processes that share the same process_state must be a reduction of the other
fact { all p, p' : Process | p.process_state = p'.process_state => (p in p'.*process_reduces_to or p' in p.*process_reduces_to) }

pred tau [p, p' : Process] {
	p.process_state = p'.process_state
	no p' & (Message.from + Message.to)
}

pred publish [p, p' : Process] {
	one m : Message |
		m.from = p' and // p' is sending a message
		m.to in p'.~process.right.process.*process_reduces_to and // it's sending it to the right
		m.payload in Pub and // the message is a Pub
		(no spawn : Process | m.payload in spawn.(p.process_state.pending_acks)) and // we don't have the pub in any pending_ack set
		(all spawn : Process |
			some (spawn.(p.process_state.pending_acks) + spawn.(p'.process_state.pending_acks)) =>
				((spawn.(p.process_state.pending_acks) = spawn.(p'.process_state.pending_acks)) or // either they're the same or we're just gaining the appropriate extra entry
				(process_spawn[spawn, p'] and ((m.payload + spawn.(p.process_state.pending_acks)) = spawn.(p'.process_state.pending_acks))))
		) and
		(one spawn : Process | process_spawn[spawn, p'] and m.payload in spawn.(p'.process_state.pending_acks)) // make sure we add the entry
}

pred receive [p, p' : Process] {
	one m : Message |
		m.to = p' and
		m.payload in Pub and
		(all spawn : Process |
			some (spawn.(p.process_state.pending_acks) + spawn.(p'.process_state.pending_acks)) =>
				(spawn.(p.process_state.pending_acks) = spawn.(p'.process_state.pending_acks) or
				(process_spawn[spawn, m.from] and ((m.payload + spawn.(p.process_state.pending_acks)) = spawn.(p'.process_state.pending_acks))))
		) and
		(one spawn : Process | process_spawn[spawn, m.from] and m.payload in spawn.(p'.process_state.pending_acks))
}

pred process_reduction [p, p' : Process] {
	tau[p, p'] <=> not (publish[p, p'] <=> not receive[p, p'])
}

fact { all p, p' : Process | p' = p.process_reduces_to => process_reduction[p, p'] }
// fact { all ps, ps' : ProcessState, p, p' : Process | (p' = p.process_reduces_to and ps = p.process_state and ps' = p'.process_state

pred example {
	#Pub > 1
	#Member > 1
	#View = 3
	#Member > 4
}
run example for 8


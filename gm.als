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
} for 5

sig Message {
	from : one Process,
	to : one Process,
	payload : one Payload
}{
	no (from & to) // if we want to send to ourself, it has to be received in the future
}

// each process can be involved in at most one send and one receive
fact { all p : Process | lone p.~from and lone p.~to }

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

// the first process can't do send and has a blank state
fact { all p : Process | (no p.~process_reduces_to) => (no (p.~from) and no p.process_state.pending_acks )}

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
/* commented out as this one requires lots else in here to be commented out
pred can_send_to_self {
	all m : Message | some (m.to.*process_reduces_to & m.from.*process_reduces_to)
	all p : Process | some (p.~from + p.~to)
}
run can_send_to_self for 6
*/

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

abstract sig Payload {}

sig Pub extends Payload {
	publisher : one Process
}{
	no publisher.~process_reduces_to
}

sig Ack extends Payload {
	pub : one Pub
}

sig ProcessState {
	pending_acks : Process -> set Pub
}{
	all p : Process | some p.pending_acks => no p.~process_reduces_to
}

// every processstate is linked to from a process
fact { ProcessState = Process.process_state }
// every pub is linked to from a message
fact { Pub + Ack in Message.payload }
// processes that share the same process_state must be a reduction of the other
fact { all p, p' : Process | p.process_state = p'.process_state => (p in p'.*process_reduces_to or p' in p.*process_reduces_to) }

pred tau [p : Process] {
	no p.~to // we can't receive anything
	no p.~from // we can't send anything
	some p.~process_reduces_to => p.process_state = p.~process_reduces_to.process_state
}

pred inject_pub [p : Process] {
	no p.~to and
	let p' = p.~process_reduces_to |
		one m : Message |
			m.payload in Pub and // the message is a Pub
			m.payload.publisher in p.*~process_reduces_to and // new publish, by us
			m.from = p and // p' is sending a message
			m.to in p.~process.right.process.*process_reduces_to and // it's sending it to the right
			(no spawn : Process | m.payload in spawn.(p'.process_state.pending_acks)) and // we didn't have the pub in any pending_ack set
			m.payload in m.payload.publisher.(p.process_state.pending_acks) and // but we do now
			equal_process_state_but_for_pub[p', p, m.payload] // and everything else is equal
}

pred receive_to_death [p : Process] {
	all p' : Process | p' in p.*process_reduces_to => (some p'.~to and p'.process_state = p.process_state and no p'.~from)
}

pred receive_pub [p', p : Process] {
	one m : Message |
		m.payload in Pub and // the message is a Pub
		m.to = p and // p receives the msg
		equal_process_state_but_for_pub[p', p, m.payload]
}

pred forward_pub [p : Process] {
	let p' = p.~process_reduces_to |
		receive_pub[p', p] and
		one m : Message |
			m.payload = p.~to.payload and
			m.from = p and
			m.to in p.~process.right.process.*process_reduces_to and
			not m.payload.publisher in p.*~process_reduces_to and // we didn't publish it
			not m.payload in m.payload.publisher.(p'.process_state.pending_acks) and // we didn't know about it before
			m.payload in m.payload.publisher.(p.process_state.pending_acks) // we do know about it now
}

pred pub_to_ack [p : Process] {
	let p' = p.~process_reduces_to |
		receive_pub[p', p] and
		some m : Message |
			m.payload in Ack and
			m.payload.pub = p.~to.payload and
			m.from = p and
			m.to in p'.~process.right.process.*process_reduces_to and
			m.payload.pub.publisher in p.*~process_reduces_to and // we published it
			m.payload.pub in m.payload.pub.publisher.(p'.process_state.pending_acks) and // it was in our pending acks
			not m.payload.pub in m.payload.pub.publisher.(p.process_state.pending_acks) // it's no longer in our pending acks
}

pred receive_ack [p', p : Process] {
	some m : Message |
		m.payload in Ack and
		m.to = p and
		equal_process_state_but_for_pub[p', p, m.payload.pub]
}

pred forward_ack [p : Process] {
	let p' = p.~process_reduces_to |
		receive_ack[p', p] and
		some m : Message |
			m.payload = p.~to.payload and
			m.from = p and
			m.to in p.~process.right.process.*process_reduces_to and
			not m.payload.pub.publisher in p.*~process_reduces_to and // we didn't publish it
			m.payload.pub in m.payload.pub.publisher.(p'.process_state.pending_acks) and // we did know about it
			not m.payload.pub in m.payload.pub.publisher.(p.process_state.pending_acks) // but we've now forgotten about it
}

pred retire_ack [p : Process] {
	no p.~from and
	let p' = p.~process_reduces_to |
		receive_ack[p', p] and
		p.~to.payload.pub.publisher in p.*~process_reduces_to and // we published it
		not p.~to.payload.pub in p.~to.payload.pub.publisher.(p.process_state.pending_acks) and // we're not pending on this
		p.process_state = p'.process_state
}

pred equal_process_state_but_for_pub [p, p' : Process, m : Pub] {
	all spawn : Process |
		some (spawn.(p.process_state.pending_acks) + spawn.(p'.process_state.pending_acks)) =>
			(spawn.(p.process_state.pending_acks) = spawn.(p'.process_state.pending_acks) or
			(spawn = m.publisher and ((spawn.(p.process_state.pending_acks) - m) = (spawn.(p'.process_state.pending_acks) - m))))
}

pred process_reduction [p : Process] {
	inject_pub[p] or
	forward_pub[p] or
	pub_to_ack[p] or
	forward_ack[p] or
	retire_ack[p] or
	receive_to_death[p] or
	tau[p]
}

fact { all p : Process | process_reduction[p] }

pred example {
	some ra: Process | retire_ack[ra] and retire_ack[ra.process_reduces_to]
	some ip: Process | inject_pub[ip] and inject_pub[ip.process_reduces_to]
	#Member = 2
	#Pub = 2
}
run example for 12 but 1 View, 2 Member

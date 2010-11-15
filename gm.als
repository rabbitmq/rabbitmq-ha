module gm

sig Member {
	left : one Member,
	right : one Member,
	member_reduces_to : lone Member
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

pred remove_rember [v, v' : View] {
	one v.members - v'.members.~member_reduces_to
	v'.members = v'.members & v.members.member_reduces_to
}

pred reduction [v, v' : View] {
	add_member[v, v'] <=> not remove_member[v, v']
}

fact { all v, v' : View | v' = v.next => reduction[v, v'] } // there must be a reduction linking consecutive views

check membership_changes_by_one {
	all v, v' : View | v' = v.next => (#v'.members = #v.members +1 or #v'.members = #v.members -1)
} for 10

sig Message {
	from : Process,
	to : Process
}{
	// somehow need to express that other communications between from and to must happen completely before or after this one
}

sig Process {
	process_reduces_to : lone Process
}{
	no (this & this.^@process_reduces_to) // no cycles
	lone this.~@process_reduces_to // at most one predecessor
}

pred example {}
run example for 0 Member, 1 View, 10 Message, 5 Process

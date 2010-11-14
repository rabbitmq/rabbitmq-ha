module gm

sig Member {
	left : one Member,
	right : one Member,
	reduces_to : lone Member
}{
	this in this.^@right // enforce a ring
	no (this & this.^@reduces_to) // no cycles
	lone this.~@reduces_to // at most one preceeding reduction
}

fact { right = ~left }

sig View {
	members : set Member,
	next : lone View
}{
	no (this & this.^@next) // no cycles
	lone this.~@next // at most one preceeding view
	no members & members.^reduces_to // members do not reduce within the same view
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
fact { all v : View, m : Member | (m in v.members and some m.reduces_to) => m.reduces_to in v.next.members }

pred addMember [v, v' : View] {
	one v'.members - v.members.reduces_to
	v.members = v.members & v'.members.~reduces_to
}

pred removeMember [v, v' : View] {
	one v.members - v'.members.~reduces_to
	v'.members = v'.members & v.members.reduces_to
}

pred reduction [v, v' : View] {
	addMember[v, v'] <=> not removeMember[v, v']
}

fact { all v, v' : View | v' = v.next => reduction[v, v'] } // there must be a reduction linking consecutive views

check membershipChangesByOne {
	all v, v' : View | v' = v.next => (#v'.members = #v.members +1 or #v'.members = #v.members -1)
} for 10

run reduction for exactly 3 View, 8 Member

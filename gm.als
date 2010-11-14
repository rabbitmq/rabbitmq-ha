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
fact { View.members = Member } // Every member is in a view.
fact { all v : View, m, m' : Member | (m in v.members and m' in v.members) => m' in m.*right } // Within a view, we have one ring only...
fact { all v : View, m : Member | m in v.members => v.members = m.*right } // ...and that ring is entirely contained within members

pred addMember [v, v' : View] {
	one v'.members - v.members.reduces_to
	v.members = v.members & v'.members.~reduces_to
}

pred reduction [v, v' : View] {
	addMember[v, v']
}

fact { all v, v' : View | v' = v.next => reduction[v, v'] } // there must be a reduction linking consecutive views

run addMember for exactly 3 View, 9 Member

pred example {}

run example for exactly 4 Member, 1 View
